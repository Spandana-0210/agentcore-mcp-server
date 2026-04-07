# MCP Server POC 

## Implementation Architecture
User Prompt
    ↓
Bedrock Agent (Claude 3.5 Sonnet)
    ↓ (LLM decides tools)
MCP Tools via AgentCore Gateway
    ↓
ECS Fargate (MCP Server)
    ├─ connect_to_vault()
    ├─ list_and_batch_keys()
    └─ migrate_batch_to_destination()
AWS Secrets Manager + KMS

## Setup
### Prerequisites
- Python 3.8 or higher
- AWS CLI configured with appropriate credentials
- AgentCore CLI installed

### Installation
1. Create a virtual environment:
```bash
python -m venv .venv
```

2. Activate the virtual environment:
```bash
# On Windows
.venv\Scripts\activate

# On macOS/Linux
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Server
```bash
python mcp_server.py
```

The server will start on `http://localhost:8000`.

### Deploying to AWS
```bash
agentcore deploy
```

This will deploy the MCP server to AWS using AgentCore's CodeBuild deployment method.

## Bedrock AgentCore Integration
### AgentCore Configuration
```aws configure```
```python -m pip install bedrock-agentcore-starter-toolkit```
```agentcore configure --entrypoint mcp_server.py --protocol MCP```
```agentcore deploy```
```agentcore status```

## Sample Prompts
```Migrate all the secrets```
```Migrate the first 3 secrets```