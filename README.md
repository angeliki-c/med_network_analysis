# med_network_analysis

Medical Network Analysis
 
 
Techniques followed

    This scenario touches upon a general approach in processing large scale heterogeneous data that exists almost
    in any industrial area and developmental sector, which involves the creation of knowledge graphs embodying
	  all the information available regarding a knowledge domain or all the information that might be needed for 
	  tackling a specific problem and, as a second step, involves building algorithms for traversing, analysing
	  and processing that graph for addressing sophisticated queries, understanding its structure, making estimations
	  and predictions, taking into consideration, apart from other features, structural features innate to the problem.
	
	  This approach is used in a large range of applications, in analyzing transaction data and detecting fraud, in
	  natural language understanding, processing textual terms connected with syntactic, grammatical rules and other
	  relationships, in social network applications and in many others. In this setting, it is going to be used for
	  analysing relationships	between medical concepts, represented by tags and keywords in medical articles, published 
	  in scientific journals, which are organised in a large database. And thus this approach is an alternative way, to 
	  one already explored, in identifying topic relatedness between text terms, yet, in this case, articles are first 
	  organised in a graph and there is the potential of answering complex queries, as much as detailed is the graph in
	  attributing relationships between nodes/entities.
	
	  Analytic techniques leveraging Graph Theory [4] are ofthen used in graph analysis, findings paths between entities, 
	  maximum/minimum flow, connected components, specific properties such as acyclic, containing cliques, complete and
	  others. Optimization algorithms [5] have been developed to make graph algorithms efficient at scale. In addition,
	  machine learning algorithms, learning on the graphical structure of the data, such as Graph Convolutional Networks
	  [6] and others are only a few of the methods that are developed, for the purpose of exploiting structural information
	  existing in various domains. 	


  
Data set

	This use case was initially inspired by [1, 3] and the data set that has been used is a sample of citation index
	data from the MedLine (Medical Literature Analysis and Retrieval System Online) database, maintained by NIH (National 
	Institute of Health). MedLine contains about 20 million academic papers, which are organized for easy discovery 
	and access of content, using keywords and tags (MeSH terms). Citation index files are in xml format and within the file
	each article title corresponds to a list of tags, MeSH terms.

    From this data set, a co-occurance data set will be created between major terms per article, in order to explore
	the relationships between topics, based on the relatedness of each topic to other topics, according to the citation 
	indices. In specific, in the case that the index of the bibliography hasn't any reference to a connection between 
	two terms, we may conclude that the terms are not related or it has not been published yet any article on their 
	connection or we may be able to identify that they may somehow be related by identifying a path that connects them 
    in the terms' graph or by another way using the data. From the co-occurance data set, a graph will be constructed
	with nodes representing the MeSH terms and edges bearing the count of times two terms appear together in the citation
    index.
	
    

[Baseline]



Challenges

	The demand for resources may grow much depended on the extent of cooccurences of the MeSH terms and the relatedness of 
	the articles, as well as for the representation of the data in a graph and the execution of graph algorithms on it.
	The distributed setting and the built-in Spark optimizations overcome many of the challenges that the volume of the data
	and the complexity of the algorithms bring in.
	
	
	
Training process

    The modelling of the data in this use case is not based on an iterative process.



Evaluation

    It has not been performed any evaluation of the analytic technique followed.
	


Performance Metrics

    In this use case, it is not evaluated the performance of the approach followed in detecting relatedness between topics in  
	the form of tags from the literature. There are many ways to further refine this approach and build an antagonizing
	commercial product or a research instrument. Though, it highlights the potential of large scale analytical frameworks and
	analytical algorithms, in identifying structural characteristics in raw data and taking advantage of them to extract insight.  

	
 
Code

   large_scale_graph_analysis.py
   
   All can be run interactively with pyspark shell or by submitting e.g. exec(open("project/location/med_network_analysis/large_scale_graph_analysis.py").read()) 
   for an all at once execution. The code has been tested on a Spark standalone cluster. For the Spark setting,
   spark-3.1.2-bin-hadoop2.7 bundle has been used.
   The external python packages that are used in this implementation exist in the requirements.txt file. Install with: 
	   pip install -r project/location/med_network_analysis/requirements.txt
   This use case is inspired from the series of experiments presented in [3], though it deviates from it, in the
   programming language, the setting used and in the analysis followed.



References

	1. Large-Scale Structure of a Network of Co-Occurring MeSH Terms: Statistical Analysis of Macroscopic Properties,
     	Andrej Kastrin, PLoS One, 2014
	2. ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/*.gz
	3. Advanced Analytics with Spark, Sandy Ryza, Uri Laserson, Sean Owen, & Josh Wills
	4. Reinhard Diestel,Graph Theory (3rd ed'n), 2005, https://sites.math.washington.edu 
	5. End to end learning and optimization on graphs, Bryan Wilder, 2019, NeurIPS 2019
	6. Graph neural networks: A review of methods and applications, Jie Zhou, AI Open, 2020
	7. Collective dynamics of small world networks, Watts, Nature, 1998
	
	
	
