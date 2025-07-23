# Snowflake Cortex Analyst Integration

This MCP server now includes integration with Snowflake Cortex, allowing agents to leverage Snowflake's built-in AI capabilities for data analysis.

## Overview

The `cortex_analyst` tool enables natural language querying of your Snowflake data. Instead of the agent needing to explore schemas and write SQL queries manually, it can now ask Cortex to analyze the data directly.

## How It Works

The tool uses Snowflake's `CORTEX.COMPLETE` function to:
1. Understand natural language questions about your data
2. Generate appropriate SQL queries
3. Execute queries and return results
4. Provide insights and recommendations

## Usage

### Basic Query
```json
{
  "tool": "cortex_analyst",
  "arguments": {
    "question": "What were the top 5 products by revenue last quarter?"
  }
}
```

### Query with Context
```json
{
  "tool": "cortex_analyst",
  "arguments": {
    "question": "Show me the trend of customer orders over the past 6 months",
    "context_tables": ["SALES.PUBLIC.ORDERS", "SALES.PUBLIC.CUSTOMERS"],
    "execute_sql": true
  }
}
```

### Advanced Options
```json
{
  "tool": "cortex_analyst",  
  "arguments": {
    "question": "Analyze customer churn patterns and suggest retention strategies",
    "context_tables": ["ANALYTICS.PUBLIC.CUSTOMER_ACTIVITY"],
    "model": "llama3.1-70b",
    "temperature": 0.3,
    "max_tokens": 8192,
    "execute_sql": false
  }
}
```

## Parameters

- **question** (required): Natural language question about your data
- **context_tables** (optional): Array of table names to provide as context
- **model** (optional): LLM model to use (default: "mistral-large2")
- **execute_sql** (optional): Whether to execute generated SQL (default: true)
- **temperature** (optional): Controls randomness (0.0-1.0, default: 0.0)
- **max_tokens** (optional): Maximum response length (default: 4096)

## Available Models

- `mistral-large2` (default)
- `llama3.1-8b`
- `llama3.1-70b`
- `llama3.1-405b`
- `gemini-1.5-flash`
- `gemini-1.5-pro`
- And more (check Snowflake documentation)

## Response Format

The tool returns a JSON response with:
- **question**: The original question
- **model**: Model used
- **answer**: Natural language answer
- **sql_query**: Generated SQL query
- **insights**: Array of additional insights
- **query_results**: Query execution results (if execute_sql=true)
- **query_error**: Any execution errors

## Requirements

1. Your Snowflake account must have Cortex enabled
2. User must have the `SNOWFLAKE.CORTEX_USER` role
3. Sufficient warehouse compute for AI operations

## Example Use Cases

### Sales Analysis
```
"What were our best performing regions last year and what factors contributed to their success?"
```

### Customer Insights  
```
"Identify customers at risk of churning based on their recent activity patterns"
```

### Inventory Management
```
"Which products have unusual inventory levels compared to their sales velocity?"
```

### Financial Reporting
```
"Generate a monthly revenue breakdown by product category with year-over-year comparison"
```

## Benefits

1. **Natural Language**: No SQL knowledge required
2. **Context Aware**: Understands business terminology 
3. **Intelligent**: Generates optimized queries
4. **Insightful**: Provides analysis beyond raw data
5. **Efficient**: Reduces back-and-forth exploration

## Error Handling

The tool includes fallback mechanisms:
1. Structured output with JSON schema (preferred)
2. Simple text completion (fallback)
3. Detailed error messages with troubleshooting hints

## Best Practices

1. **Be Specific**: Clear, specific questions yield better results
2. **Provide Context**: Include relevant table names when known
3. **Start Simple**: Test with straightforward queries first
4. **Review SQL**: Always review generated SQL before production use
5. **Use Appropriate Models**: Larger models for complex analysis

## Limitations

- Token limits may restrict very large context or responses
- Complex multi-step analyses may need breaking down
- Generated SQL should be reviewed for production use
- Performance depends on warehouse size and model choice