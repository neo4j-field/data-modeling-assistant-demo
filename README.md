# Data Modeling Assistant Demo

This project demonstrates how to use the Neo4j Data Modeling MCP server to assist in modeling relational data as a graph and ingesting into a Neo4j database.

## Tools Used

* [Claude Code](https://www.anthropic.com/claude-code)
* [Claude Desktop](https://claude.ai/download)
* [Neo4j Cypher MCP Server](https://github.com/neo4j-contrib/mcp-neo4j/tree/main/servers/mcp-neo4j-cypher)
* [Neo4j Data Modeling MCP Server](https://github.com/neo4j-contrib/mcp-neo4j/tree/main/servers/mcp-neo4j-data-modeling)

## Relational Data Generation

Claude Desktop was used to generate mock Salesforce data that we can use for our modeling exercise. No other external tooling was used to generate the data. 

The generated CSV files were then copied to the data/ directory of this project.

You can read more about how the data was generated in the [data README](./data/README.md).

## Graph Data Modeling

The Neo4j Data Modeling MCP server was used to facilitate the analysis and creation of a graph data model based on our mock Salesforce data. 

We used the `create_new_data_model` prompt provided by the MCP server and passed the `data_description.txt` and `use_cases.txt` content as prompt parameters. No desired nodes or relationships were included in the prompt.

Process
* Analyze the CSV files
* Check for relevant example data models in MCP server
* Read and analyze relevant example data models
* Generate data model
* Validate data model
* Visualize data model with Mermaid
* Address any user feedback

## Ingest Script Generation

Claude Desktop using Claude Sonnet 4 was used to generate a YAML file organizing our Cypher ingest queries. The prompt found in `yaml_gen.txt` was used for this process.

Claude Code was used to generate our ingestion script. The prompt found in `code_gen.txt` was used for this process. Claude Code was able to 2-shot this ingest script and our data was successfully loaded. Note that the relationship (:Lead)-[:CONVERTED_TO_OPPORTUNITY]->(:Opportunity) was identified as not being possible given the provided data. We are missing data that describes whether a lead was converted to an opportunity. Therefor our graph is missing this relationship.

### Running The Ingest

In order to run the ingest script you must first install the dependencies. We use the uv package manager in this project.

```bash
uv sync
```

You will also need a `.env` file with the proper environment variables to connect to a Neo4j instance. Please see the `.env.example` file for reference.

Then we can run the ingest script.

```bash
uv run python3 ingest.py
```

### Ingest Results

| Node Label | Count |
|-----------|-------|
| Account | 25 |
| Contact | 31 |
| Case | 30 |
| Opportunity | 30 |
| Lead | 20 |
| CaseOwner | 25 |

| Relationship Type | Count |
|-------------------|-------|
| BELONGS_TO_ACCOUNT | 31 |
| HAS_CASE | 30 |
| REPORTED_BY | 30 |
| HAS_OPPORTUNITY | 26 |
| ASSIGNED_TO | 30 |

## Use Case Validation

Claude Desktop using Claude Sonnet 4 was used to analyze our use cases with the resulting Neo4j graph. We used the Neo4j Cypher MCP server to connect to the database. 

The following is the AI analysis of our use cases. Please see the [use case validation transcript](./transcripts/use_case_validation.txt) and [validation Cypher queries](./validation_config.yaml) for more detail.

✅ Use Case Validation Results
Your Neo4j graph successfully addresses both critical use cases. Here's what I found:

🚨 High-Risk Accounts (Open Tickets + Active Opportunities)
Critical Risk Accounts (require immediate attention):

BioPharma Research ($2.57M in opportunities) - High-priority compliance case with Jennifer Lee
Financial Partners LLC ($2.14M opportunities) - High-priority SSO implementation with Anna Thompson
MediaStream Networks ($1.54M opportunities) - Medium-priority mobile compatibility issue
RetailMax Stores ($1.10M opportunities) - ESCALATED POS integration issue with Lisa Wang
Key Risk Indicators:

16 accounts have both open support tickets AND active opportunities
Combined opportunity value at risk: $13.7M+
4 accounts have "Hot" rating with high-priority cases
1 case is already escalated (RetailMax Stores)
📊 Case Assignment Recommendations
For New Case Assignment, Consider:

Workload Balance:

Available capacity: Michelle Davis, Kevin Brown, Eric Johnson (0 open cases)
Moderate load: Most owners have 1 open case
High load: Emily Rodriguez (2 open cases, 1 critical)
Industry Expertise Mapping:

Technology: 4 specialists (Sarah Johnson, Rachel Green, Maria Garcia, Daniel Kim)
Healthcare: 2 specialists (Emily Rodriguez - overloaded, Lauren White - available)
Financial Services: 2 specialists (Anna Thompson, Nicole Adams)
Retail: 2 specialists (Lisa Wang, Amanda Chen)
Recommended Assignment Logic:

cypher

```cypher
// For a new Technology case, prefer available tech specialists
MATCH (owner:CaseOwner)<-[:ASSIGNED_TO]-(c:Case)<-[:HAS_CASE]-(a:Account)
WHERE a.industry = 'Technology'
WITH owner, count(CASE WHEN c.status <> 'Closed' THEN 1 END) as workload
ORDER BY workload ASC
RETURN owner.name, workload
```
🎯 Validation Summary
✅ Your graph successfully enables:

Risk identification - Easy to spot accounts with open tickets and active deals
Workload balancing - Clear visibility into case owner capacity
Industry expertise - Track which owners handle which industries
Account familiarity - See which owners have history with specific accounts
💡 Recommendations for new case assignment:

Healthcare cases → Lauren White (Emily Rodriguez overloaded)
Technology cases → Daniel Kim or Maria Garcia (lowest current workload)
New industries → Michelle Davis, Kevin Brown, or Eric Johnson (available capacity)
Would you like me to create specific Cypher queries for automated case assignment based on these criteria, or dive deeper into any particular aspect of the analysis?



