# data-modeling-assistant-demo

This project demonstrates how to use the Neo4j Data Modeling MCP server to assist in modeling relational data as a graph and ingesting into a Neo4j database.

## Relational Data Generation

Claude Desktop was used to generate mock Salesforce data that we can use for our modeling exercise. No other external tooling was used to generate the data. 

The generated CSV files were then copied to the data/ directory of this project.

You can read more about how the data was generated in the [data README](./data/README.md).

## Graph Data Modeling

We can use the Neo4j Data Modeling MCP server to facilitate the analysis and creation of a graph data model based on our mock Salesforce data. 

