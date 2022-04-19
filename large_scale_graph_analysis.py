"""
    - read the data
    - data preprocessing: retrieve the MesH terms of major topic per article
    - statistical summary of the data
    - create the 'vertices' dataframe   
    - create the 'edges' dataframe 
    - create a graph G(V,E), V: the set of nodes representing MedLine topics, E: the set of relatedness 
      connections formed between the topics in the data set 
    - network structural analysis
    - apply an alternative weighting scheme on the edges, for removing connections between topics that are not of that interest,
      such as those representing hierarchical relations, in terms of specificity, between concepts
    - network structural analysis on the new graph created
    - investigate if the network fulfills the 'small network' properties 
    - query the graph (find the shortest path between two concepts, find the average distance to each node, motif finding e.t.c.)

"""


SparkContext.setSystemProperty('spark.executor.memory','4g')
SparkContext.setSystemProperty('spark.driver.memory','4g')
SparkContext.setSystemProperty('spark.master','local[2]')
    
    
# read the data from hdfs fs into a Spark dataframe
df = spark.read.format('xml').option('inferSchema','true').option('rootTag','MedlineCitationSet').option('rowTag','MedlineCitation').load('hdfs://localhost:9000/user/data/*.xml')

from pyspark.sql.types import StringType
from pyspark.sql import Row

def transform(r):
    topic_list = []
    if (r['MeshHeadingList'] != None):
        if (r['MeshHeadingList']['MeshHeading'] != None) & (len(list(r['MeshHeadingList']['MeshHeading'])) != 0):
            for ro in r['MeshHeadingList']['MeshHeading']:
                if ro['DescriptorName'] !=None:
                    if (ro['DescriptorName']['_VALUE'] != None) & (ro['DescriptorName']['_MajorTopicYN'] == 'Y'):
                        topic_list.append( ro['DescriptorName']['_VALUE'] )

    if len(topic_list) == 0:
        return None
    else:
	    return Row(r['PMID']['_VALUE'],topic_list)

# if you have enough memory resources, it is a good practice to cache the dataframe, if it is going to be used in subsequent computations
#df = df.cache()
#df.count()    #  240000
# parse the xml and extract the 'pmid' of the scientific article and the MeSH headings, which are major topics.
dft = df.select(['PMID','MeshHeadingList']).rdd.map(lambda r : transform(r) if transform(r) != None else Row(None,None)).toDF(['pmid','topics'])    
#df.unpersist()
#dft = dft.cache() 
dft = dft.dropna(how = 'any')                           
#dft.count()                          
# check that the id is unique 
#dft.select('pmid').distinct().count()    # it is equal to dft.count(), it is unique

import pyspark.sql.functions as F
# expand the list of topics that appear per article and create a (pmid, topic) data set
df_f = dft.rdd.flatMap(lambda r : [Row(r['pmid'], el) for el in list(r['topics'])]).toDF(['pmid','topic'])   

# if you are struggling with memory resources, repartitioning the dataframe at a greater number of partitions, 
# may do the trick, when doing memory intensive computations 
df_f.rdd.getNumPartitions()
df_f = df_f.repartition(27)
#df_f = df_f.cache()
#df_f.count()         
print("==================")
num_topics = df_f.groupBy('topic').count().count()
print(f"There are {num_topics} different topics in the dataset. ")
print("==================")
print("The distribution of the topics in the dataset: ")
dist = df_f.groupBy('topic').count().orderBy(F.desc('count')).selectExpr(['topic','count as dist'])
dist.show(truncate = False)
print("==================")
print(f"""The most frequent topics, unsurprisingly, are some of the most general ones.
          Given that the numbers of different topics in the dataset is {num_topics}, it
          is possible that the distribution has long tail.""")
print("==================")
print('Frequency count on the topic distribution:')
dist.groupBy('dist').count().orderBy(F.desc('count')).selectExpr(['dist','count as cnt']).show(truncate = False)
         
dft.rdd.getNumPartitions()
dft = dft.repartition(27) 
import itertools
# Based on the number of different topics in the dataset which is currently 14548
# the estimated number of combined pairs of topics are  #combinations = #mesh_tags (#mesh_tags - 1)/2 = 105,814,878 pairs.
df_p =  dft.rdd.flatMap(lambda r : [Row(r['pmid'], list(el)) for el in itertools.combinations(sorted(list(r['topics'])),2)]).toDF(['pmid','pairs'])   
df_p = df_p.rdd.map(lambda r :Row(r['pmid'], sorted(list(r['pairs']))) ).toDF(['pmid','pairs'])  
#dft.unpersist()  
print("==================")
print("The number of co-occurance pairs of topics in the dataset : ")	
df_p.groupBy('pairs').count().orderBy(F.desc('count')).select(F.count('pairs')).show(truncate = False)   #  213,745 out of 105,814,878 possible co-occurances appear in our dataset																					
#df_p = df_p.cache()
print("==================")
print("The distribution of the co-occurance pairs : ")	
df_p.groupBy('pairs').count().orderBy(F.desc('count')).select(['pairs','count']).show(truncate = False) 																																
# The most frequent pairs of topics.
# It doesn't provide us with much information examining only the extremes of the co-occurances.
# Lets create the graph of the vertices (topics) and edges (the co-oocurance of two topics in abs
# citation record)
        																						   
# Construct the graph

# the vertices

df_f_r = df_f.rdd.map(lambda r : (r['topic'],r['pmid'])).groupByKey().mapValues(list).toDF(['topic','pmid_list'])   
#df_f.unpersist()
from pyspark.ml.feature import StringIndexer
# the vertex id column should be named 'id' in the graphframe framework
indexer = StringIndexer().setInputCol('topic').setOutputCol('id')                     
im = indexer.fit(df_f_r)
vertices = im.transform(df_f_r)
vertices = vertices.cache()
vertices = vertices.withColumn('id',vertices.id.cast('integer'))                         

def topic2index(vertices):
    topic2index = dict()
    vert_list = vertices.collect()
    topic2index = {r['topic'] : r['id'] for r in vert_list}
	
    return  topic2index
	
def index2topic(vertices):
    index2topic = dict()
    vert_list = vertices.collect()
    index2topic = {r['id'] : r['topic'] for r in vert_list}
	
    return  index2topic 
    
t2i = topic2index(vertices)
i2t = index2topic(vertices)
# the 'topic' collumn may be dropped, for saving some resources
vertices = vertices.select(['id'])
vertices = vertices.cache()
# some information on the 'vertices' dataframe 
vertices.count()                     #   14548    
vertices.select('id').distinct().count()   # the index is indeed unique for any topic
                               
# the edges
df_p_g = df_p.groupBy('pairs').count().orderBy(F.desc('count')).select(['pairs','count'])    
edges = df_p_g.rdd.map(lambda r : Row(t2i.get(r['pairs'][0]), t2i.get(r['pairs'][1]), r['count'])).toDF(['src','dst','cnt'])
edges = edges.cache()
# here we keep the convention that src index should be smaller than the dst index
edges = edges.rdd.map(lambda r : Row(r['cnt'],r['src'] if r['src']<= r['dst'] else r['dst'],r['src'] if r['src']> r['dst'] else r['dst'] )).toDF(['cnt','src','dst'])  
# Thus, the weight of an edge corresponds to how often these concepts co-occure in the citations in the whole set of the papers.
edges = edges.cache()    
edges.count()            #   213745 edges                                                    
																		
														
from graphframes import *

g = GraphFrame(vertices,edges)                                    
g = g.cache()									 
									 
# Understanding the structure of the network	
print("==================")							 
print("The graph is ready. Understanding the network ....")
sc.setCheckpointDir('./checkpoints')
# Identify the connected components in the graph. The graph may be connected or composed of a set of connected subgraphs.
# The graph is assumed to be undirected, when calculating the connected components. This assumption doesn't hold when we call the StronglyConnectedComponents() method.
cc = g.connectedComponents()                              

# Size of each component in terms of vertices/topics belonging to it.
# num of components in the graph = 878
ccg = cc.groupBy(['component']).count().orderBy(F.desc('count')) 
num_components = ccg.count() 
print("==================")
print(f"The connected components in the graph and the size of each component in number of nodes included, in decreasing order. The total number of the connected components in the graph is : {num_components}")
#ccg = ccg.cache()                      
ccg.show(truncate = False) 
print("==================")
print("Summary statistics on the distribution of the connected components in the graph:")
ccg.describe().show()                                                

# There is one large component of 13610 vertices and the rest are isolated components of topics/vertices.
# The vertices of the majority component do not correspond only to the most general topics. 
print("==================")
print("A sample of the topics included in the biggest component of the graph.")
cc_with_topics = cc.withColumn('topic',F.udf(lambda i : i2t[i])(F.col('id')))
biggest_component_id = ccg.take(1)[0].component																	
cc_with_topics.where(f'component = {biggest_component_id}').show(truncate = False)  
print("==================")
print("Lets examine the topics included in different components from the major that their size is significantly smaller than that of the major, almost at 1000 times:")
clist = ccg.where(f'count <{ccg.first()[1]/100}').take(3)
for r in clist:
    cid = r.component
    cc_with_topics.where(f'component = {cid}').select(['component','topic']).limit(10).show(truncate = False)
                 			    
print("==================")                                                     			 
print("Do we have similar topics belonging to different components ? ")
cc_with_topics.where('topic like "%Conotoxins%"').show(truncate = False)
cc_with_topics.where('topic like "%Sodium Channel%"').show(truncate = False)
cc_with_topics.where('topic like "%Antagonists%"').show(truncate = False)
print("==================")                                                     			 
print(""" Therefore, there are topics that could be related, though they appear in different components. 
          This is an indication that the weighting scheme for the edges we are using, is not able to
          represent relatedness of concepts at the generic level we might wish or an indication that in 
          fact these concepts despite looking affiliated they are not or simply there is not yet any article
          that connects them in the knowledge graph.""")
          
          
          
print("==================")
print("Lets examine the topics included in different components from the major that their size is significantly smaller than that of the major, almost at 1000 times:")
clist = ccg.where(f'count <{ccg.first()[1]/100}').take(3)
for r in clist:
    cid = r.component
    cc_with_topics.where(f'component = {cid}').select(['component','topic']).limit(10).show(truncate = False)
    
# The distribution of vertices' degree provides us information on the structure of the graph
# e.g. one can discern from the vertice degrees, whether the graph has cycles or whether it has an asteroid shape and so on.	
print("==================")   
print("The distribution of the connectivity degree across vertices:")				
degree_dist = g.degrees.groupBy('degree').count().orderBy(F.desc('count')).selectExpr(['degree','count as cnt'])
degree_dist.show(truncate = False)              
g.degrees.count()                       #  13721 < 14548, some vertices are isolated.	
print("==================")
print("Summary statistics on the distribution of the connectivity degree of the vertices in the graph:")
degree_dist.describe().show()  	
print("==================")
print(""" The vast majority of the nodes exhibits mediocre degree of connection to other vertices. Though, there are 
      few nodes that are more than 10 times more connected than the others. There are also nodes that are isolated. img.png, 
      in the current directory, shows the curve of this relation.""")	
import numpy as np
import matplotlib.pyplot as plt
degrees =  degree_dist.collect()
degrees_np = np.array([[int(r.degree), int(r.cnt)] for r in degrees])  
plt.figure()
plt.plot(np.linspace(0,max(degrees_np[:,0]), len(degrees)),degrees_np[:,1])
plt.ylabel('num of vertices with that degree')
plt.xlabel('Degree')
plt.savefig('img')
plt.close()
print("==================")
print('The most connected nodes most probably correspond to general topics e.g.: ')

hc_id = g.degrees.where('degree >2000').collect()[0].id
g.vertices.where(f'id = {hc_id}').withColumn('topic',F.udf(lambda i : i2t[i] )(F.col('id'))).show(truncate = False)
print("==================")
print("There are some nodes that appear to be isolated, as they do not exist in the 'degrees' dataframe of the graph. These are :")
g.degrees.createOrReplaceTempView('degrees')
g.vertices.createOrReplaceTempView('vertices')
isolated_vertices = spark.sql("select vertices.id from vertices  where vertices.id not in (select degrees.id from degrees) ")
isolated_vertices.withColumn('topic',F.udf(lambda i : i2t[i] )(F.col('id'))).show(truncate = False)

# Creation of a new graph after removing the isolated vertices.

g1 = g.dropIsolatedVertices()			 
g1 = g1.cache()
g.unpersist()


#Filtering out noisy edges


"""The weighting scheme applied so far for estimating the relatedness of the topics was based on the count of co-occurance
   of two terms in the dataset. In the following analysis we are going to try another weighting scheme that is based 
   on the Pearson Chi-squarred test. With this test we will attempt to remove all those topics that just happen to co-occure,
   without in essence being related to each other and actually consist noise for the set of related topics we try to make 
   (following [3] 's approach).
 
   For the chi-squared test, we think of YY, YN, NY, and NN (from the contigency matrix) as sampled from an
   unknown distribution. We can compute a chi-squared statistic from these values with: 

   χ^2 = T [(YY * NN − YN * NY − T/2)^2 ]/(YA * NA * YB * NB) for each pair of concepts.         
   The term -T/2 is the Yates's continuity correction (it may or may not be included).

   If the topics in the pair of topics are independent we expect that the value of this Chi-squarred statistic to be drown
   from a chi-squarred distribution with the appropriate degrees of freedom (2x2 contigency table=>1 d.f.). If the statistic
   is too large the concepts are less likely to be independent.
"""

# Calculation of the Pearson chi-squarred statistic 
T = df.count()                                          #   The num of all the documents (potential references of the concepts).
#   We are going to create a new graph tat is going to facilitate the computation of the statistic.
vertices2 = df_f.groupBy('topic').count().selectExpr(['topic','count as cnt'])                                                                  
vertices2 = vertices2.cache()
indexer.setInputCol('topic').setOutputCol('id')
vertices2 = indexer.fit(vertices2).transform(vertices2)
vertices2 = vertices2.withColumn('id',vertices2.id.cast('integer'))
vertices2 = vertices2.cache()
t2i_2 = topic2index(vertices2)
i2t_2 = index2topic(vertices2)
vertices2 = vertices2.select(['id','cnt'])
vertices2 = vertices2.cache()

edges2 = df_p_g.rdd.map(lambda r : Row(t2i_2.get(r['pairs'][0]), t2i_2.get(r['pairs'][1]), r['count'])).toDF(['src','dst','cnt'])
edges2 = edges2.cache()
edges2 = edges2.rdd.map(lambda r : Row(r['cnt'],r['src'] if r['src']<= r['dst'] else r['dst'],r['src'] if r['src']> r['dst'] else r['dst'] )).toDF(['cnt','src','dst'])    #  keep the convention
edges2 = edges2.cache()

g2 = GraphFrame(vertices2,edges2)             #    A new graph containing the new set of vertices and the existing set of edges.
g2 = g2.dropIsolatedVertices()

g2 = g2.cache()

import math

def pearson_chi_square(r): 
    yy = r['edge']['cnt']
    yn = r['src']['cnt'] - yy
    ny = r['dst']['cnt'] - yy
    ya = r['src']['cnt']
    na = T - ya
    yb = r['dst']['cnt']
    nb = T - yb
    nn  = T - yy -yn - ny
    chi_sq = T * ((math.fabs(yy * nn - yn*ny) - T/2) ** 2)/(ya*na*yb*nb)
    new_r = Row(src=r['src'],edge = Row(cnt = chi_sq,src = r['edge']['src'],dst = r['edge']['dst']),dst = r['dst'])
	
    return new_r
	
triplets = g2.triplets.rdd.map(lambda r : pearson_chi_square(r)).toDF(['src','edges','dst'])
triplets = triplets.cache()
vertices_a = triplets.rdd.map(lambda r : Row(cnt = r['src']['cnt'], id = r['src']['id'])).toDF(['cnt','id'])
vertices_b = triplets.rdd.map(lambda r : Row(cnt = r['dst']['cnt'], id = r['dst']['id'])).toDF(['cnt','id'])
vertices_cs = vertices_a.union(vertices_b).distinct()
vertices_cs = vertices_cs.cache()

edges_cs = triplets.rdd.map(lambda r : Row(cnt = r['edges']['cnt'], src = r['edges']['src'],dst = r['edges']['dst'])).toDF(['cnt','src','dst'])
edges_cs = edges_cs.cache()

g_cs_temp = GraphFrame(vertices_cs,edges_cs)                               #   this is a new graph where the edges have weights attributed by a weighting 
g_cs_temp = g_cs_temp.cache()											   #   scheme that uses the pearson chi-squared statistic
g1.unpersist()
g2.unpersist()

print("==================")
print(""" As an attempt to filter out noisy data, a new weighting scheme will be used for the edges that is based on Chi-Squared statistic. 
          A new graph will be created and that graph's edges will be filtered, removing edges with weight <= 19.5. The edges of that weight 
          based on the Chi Squared independence test are probably independed each other (the presense of the one topic doesn't mean the 
          presense of the other in the pair ).""")


"""For a 2x2 contigency matrix where there is no relationship between the 2 vars we expect that the value of the chi-squarred statistic will 
follow the chi-squared distribution with one degree of freedom.
The 99.997 percentile for this distribution corresponds to the value 19.5 for this statistic. Therefore for discrepencies, where the statistic
is equal or greater 19.5, the variables are considered by approximately 0.003, dependent each other and the null hypothesis (concepts are
independent) is not probable to hold.
Therefore for filtering out the edges that connect idependent topics together, lets keep only those edges with edges.count>19.5.  
 (https://keisan.casio.com/exec/system/1180573197 )
"""
from pyspark.sql.types import ArrayType

removed_edges = g_cs_temp.filterEdges('cnt<=19.5').edges.collect()
re_df = spark.createDataFrame([[i2t_2[r.src],i2t_2[r.dst]] for r in removed_edges],ArrayType(StringType()),'value')
re_df = re_df.cache()
re_list = re_df.collect()

g_cs = g_cs_temp.filterEdges('cnt>19.5')
g_cs = g_cs.cache()

num_vertices_cs = g_cs.vertices.count()
num_edges_cs = g_cs.edges.count()

print("==================")
print(f"""The new graph is composed of {num_vertices_cs} vertices and {num_edges_cs} edges. {g_cs_temp.edges.count() - num_edges_cs} 
       less edges and in contrast to the {g1.edges.count()} of the previous weighting scheme.""")

print("==================")
print("A sample of the pairs that were removed from the graph as probably not having an informative relation to each other: ")
re_df.show(50, truncate = False)
cc_cs=g_cs.connectedComponents()
# Size of each component in terms of vertices/topics belonging to it.
ccg_cs = cc_cs.groupBy('component').count().orderBy(F.desc('count'))
num_components_cs = ccg_cs.count()
print("==================")
print(f"The connected components in the graph and the size of each component, in number of nodes included, in decreasing order. The total number of the connected components in the graph is : {num_components_cs}.")
ccg_cs.show(truncate = False)
print("==================")
print("Summary statistics on the distribution of the connected components in the graph:")
ccg_cs.describe().show()
print("A sample of the topics included in the biggest component of the graph.")
ccs_with_topics = cc_cs.withColumn('topic',F.udf(lambda i : i2t_2[i])(F.col('id')))
biggest_component_id_cs = ccg_cs.first()[0]																	
ccs_with_topics.where(f'component = {biggest_component_id_cs}').show(truncate = False)   
print("==================")
print("Lets examine the topics included in different components from the major that their size is significantly smaller than that of the major, almost at 1000 times:")
clist_cs = ccg_cs.where(f'count <{ccg_cs.first()[1]/100}').take(3)
for r in clist_cs:
    cid = r.component
    ccs_with_topics.where(f'component = {cid}').select(['component','topic']).limit(10).show(truncate = False)            			    
print("==================")                                                     			 
print("""Do we have similar topics belonging to different components ? It seems that most topics are organized in one component.
         This is a characteristic of real-world nets, forming locally dense structures.""")
ccs_with_topics.where('topic like "%Conotoxins%"').show(truncate = False)
ccs_with_topics.where('topic like "%Sodium Channel%"').show(truncate = False)
ccs_with_topics.where('topic like "%Antagonists%"').show(truncate = False)

print("==================")   
print("The distribution of the connectivity degree across vertices:")	
degree_dist_cs = g_cs.degrees.groupBy('degree').count().orderBy(F.desc('count')).selectExpr(['degree','count as cnt'])																					   
degree_dist_cs.show(truncate = False)              
g_cs.degrees.count()                       #  13721, there are not any isolated vertices.		
print("==================")
print("Summary statistics on the distribution of the connectivity degree of the vertices in the graph:")
degree_dist_cs.describe().show()

print("==================")
print(""" The vast majority of the nodes exhibits mediocre degree of connection to other vertices. Though, there are 
          few nodes that are more than 40 times more connected than the others. There aren't any isolated nodes in 
          the graph. img_chi.png, in the current directory, shows the curve of this relation.""")	

degrees = degree_dist_cs.collect()
degrees_np = np.array([[int(r.degree), int(r.cnt)] for r in degrees])  
plt.figure()
plt.plot(np.linspace(0,max(degrees_np[:,0]), len(degrees)),degrees_np[:,1])
plt.ylabel('num of vertices with that degree')
plt.xlabel('Degree')
plt.savefig('img_chi')
plt.close()
print("==================")
print("Some of the most connected nodes in the graph correspond to general topics.")
hc_ids = g_cs.degrees.orderBy(F.desc('degree')).take(3)
hc_ids = [(int(r.id),r.degree) for r in hc_ids]
for hc_id, degree in hc_ids:
    g_cs.vertices.where(f'id = {hc_id}').withColumn('topic',F.udf(lambda i : i2t_2[i] )(F.col('id'))).withColumn('degree',F.lit(degree)).select(['topic','degree']).show(truncate = False)


#Small-world property
print("==================")
print(" Examining whether the 'small-world' property holds in the graph.")
print('Computing the local clustering co-efficient, a metric for estimating local density per vertex in a graph.')

"""
Most real world nets are characterized by the small-world property.
There are two conditions that must be met in order the small-world property to be fulfilled.                  			
Small-world nets:    1) Most nodes in the graph have a small degree of connectivity and most nodes belong to a relatively dense cluster of nodes   
                     2) Despite these 2 properties of small-world nets, it is possible to traverse the graph from one node to any other node 
                        in just a few edges.

In small-world nets we see often cliques (complete subgraphs).

The problem of detecting whether or not a given graph has a clique of a given size is
NP-complete.

A simpler metric of local density is the triangle count in a vertex, V. The triangle count is a measure of how many neighbors of V are connected each other. 

local clustering coefficient  = 2t/k(k -1)    , t is the #triangles at V, k is the #neighbors of V, graph g is undirected


"""


vertices_ren = g_cs.vertices.toDF(*[ 'plithos', 'id'])        #   lets calculate the local clustering coefficient for each vertex
                                                                    #   and the average local clustering coefficient of the network
                                                                    #   before and after removing the edges
     
                                                                    #   try rename cols because triangleCount() hits an error

vertices_ren = vertices_ren.cache()
edges_ren = g_cs.edges.toDF(*['metric', 'src', 'dst'])
edges_ren = edges_ren.cache()
g_cs = GraphFrame(vertices_ren, edges_ren)
g_cs = g_cs.cache()

tr_count = g_cs.triangleCount()
tr_count = tr_count.cache()
print("==================")
print("Triangle count per vertex.")
tr_count.show(truncate = False)

degrees_cs = g_cs.degrees
degrees_cs = degrees_cs.cache()
temp = degrees_cs.rdd.map(lambda   r : Row(r['id'],r['degree'],r['degree']*(r['degree'] - 1)/2)).toDF(['id','degree','pot_triangles'])
joined = tr_count.join(temp,'id')
joined = joined.cache()
res = joined.rdd.map(lambda r : Row(r['plithos'],r['id'],r['count']/r['pot_triangles'] if r['pot_triangles'] != 0 else None)).toDF(['plithos','id','local_clustering_coef'])
res = res.cache()
res = res.dropna(how = 'any')
res = res.cache()

from pyspark.sql.functions import round

res = res.withColumn('local_clustering_coef',round(res.local_clustering_coef, 3))
res = res.cache()
print("==================")   
print("The distribution of the local clustering coefficient across vertices:")	
res_dist = res.groupBy('local_clustering_coef').count().orderBy(F.desc('local_clustering_coef'))
res_dist.show(truncate = False)
print("==================")   
print("Summary statistics on local clustering coefficient distribution:")
res_dist.describe().show(truncate = False)
#  average 'local_clustering_coef' considering all the vertices of the network
avg_local_clustering_coef = res.agg(F.mean('local_clustering_coef').alias('avg_lcc')).collect()[0].avg_lcc                                          
print("==================")   
print(f" The average local_clustering_coef is {avg_local_clustering_coef}\n\n")																									   

# Real world small networks exhibit similar and peculiar properties. It has a particular structure at each 
# case and deviation from this is indicative of a function deficiency or other situations of the system 
# described by the network.

print("==================") 
print("""Lets compute the average distance, in number of edges, within which a vertex may be accessible in 
      the graph and check whether the second property of small-networks holds as well.""")
																							
from graphframes.lib import Pregel

subgraph_ver = g_cs.vertices.alias('a').join(cc_cs.alias('b'), on = F.expr(f'component = {biggest_component_id_cs} and a.id = b.id')).selectExpr(['a.id','a.plithos'])
g_css = GraphFrame(subgraph_ver, g_cs.edges)
g_css = g_css.cache()
# average path between nodes
avg_pr = g_css.pregel.withVertexColumn('avg_path',F.lit(0),F.coalesce(Pregel.msg(),F.lit(0))).sendMsgToDst(1 + Pregel.src('avg_path')).aggMsgs(F.avg(Pregel.msg())).run()   																																						   
avg_pr = avg_pr.cache()
print("==================") 
print("Using the pregel framework, the average path for each vertex in the major component is:")
avg_pr.show(truncate = False)
print("==================") 
print("Summary statistics on the average path data:")
avg_pr.describe().show(truncate = False)
print("==================") 
print("""Lets compute the shortest paths that exist between two nodes/topics of the major connected component 
         in the graph, using the BFS algorithm.""")
	
print(f"paths between '{from_topic}' and '{to_topic}':")

# shortest paths between two topics
from_topic = 'Chromosomes'
to_topic = 'Proteins'
paths = g_css.bfs(f'id = {t2i_2[from_topic]}', f'id = {t2i_2[to_topic]}')


def convert(r):
    path = []
    for i,field in enumerate(r):
        if i % 2 == 0:
            path.append(i2t_2[field.id])
            
    return path 
    
paths.withColumn('path_of_topics', F.udf(lambda r : convert(r))(F.struct([F.col(col) for col in paths.columns]))).show(truncate = False)


       
    

