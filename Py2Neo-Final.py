import py2neo
from py2neo import Graph, Node, Relationship
import pandas as pd


#py2neo.authenticate("localhost:7474", "username", "password")
py2neo.authenticate("localhost:7474", "neo4j", "123456")

graph=Graph("http://localhost:7474/db/data/")
	
#load the data from CSV into Neo4j
results = graph.run("""USING PERIODIC COMMIT
					LOAD CSV WITH HEADERS 
					FROM ('file:///lexiconout.csv') AS line
					MERGE (a1:Author { name: (line.authorname) })
					MERGE (a:Airline { airline: (line.airlinename) })""")
for result in results:
	print(results)
	
#create constraints
results = graph.run("""CREATE CONSTRAINT on (a1:Author) ASSERT a1.authorname is UNIQUE""")
for result in results:
	print(results)

results = graph.run("""CREATE CONSTRAINT on (a:Airline) ASSERT a.airlinename is UNIQUE""")
for result in results:
	print(results)

#Find airline competitors rated by one person (Part 2)
results = graph.run("""match (a:Airline)<-[r:REVIEWS]-(u:Author)
					with distinct a.airline as name, max(r.rating) as max_rating, u.name as Author, r.recommendation as recommend
					with Author, collect([name,max_rating]) as Airline, recommend
					where size(Airline) > 1 and recommend = '1'
					return Author, Airline
					ORDER BY Author""")

for result in results:
    print(result)

#Find common interest rates for airlines with average rating for every airline   
results = graph.run("""match (a1:Airline) - [r:REVIEWS] - (a:Author)
					where r.recommendation > '0'
					return a1.airline,collect(a.name), avg(toint(r.rating))
					LIMIT 2000""") 

for result in results:
    print(result)

#Find common interest raters for false negative sentiment   
results = graph.run("""match (a:Airline) <- [r:REVIEWS] - (b:Author)
					where r.recommendation = '1' and b.name = "A Amaladoss"
					return distinct b.name as Author, a.airline as Airline, r.sentiment as Sentiment, r.rating as Rating, r.recommendation as Recommended, r.review as Review
					ORDER by b.name""")

for result in results:
    print(result)
    
#Find sentiment between different competing airlines
results = graph.run("""match (a1:Airline) - [r:REVIEWS] - (a:Author) - [r1:REVIEWS] - (a2:Airline) 
					return a.name as AuthorName, a1.airline as Airline, r.rating as Rating, collect([a2.airline, r1.rating]) as Competitor
					LIMIT 100""")

for result in results:
    print(result)
    
