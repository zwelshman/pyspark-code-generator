import streamlit as st
import anthropic
import os

st.set_page_config(page_title="PySpark Code Generator", page_icon="⚡", layout="wide")
st.title("PySpark Code Generator")
st.markdown("Enter your data manipulation instructions and get clean PySpark code")

# API key input
api_key = os.getenv("ANTHROPIC_API_KEY", "")

# Instructions input
instructions = st.text_area(
    "Instructions",
    placeholder="E.g., Filter patients where age > 65, select id and diagnosis, group by diagnosis and count",
    height=150
)

if st.button("Generate Code", type="primary"):
    if not api_key:
        st.error("Please enter your Anthropic API key")
    elif not instructions.strip():
        st.error("Please enter instructions")
    else:
        with st.spinner("Generating code..."):
            try:
                client = anthropic.Anthropic(api_key=api_key)
                
                message = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=2000,
                    system="""You are an expert PySpark code generator. Generate Python code using PySpark DataFrame API for distributed data processing.
            
CRITICAL OUTPUT REQUIREMENTS:
- Return ONLY executable Python code
- NO markdown code blocks (no ```python or ```)
- NO explanations, comments, or preamble
- NO natural language before or after the code
- Start directly with Python code (e.g., 'df.filter(' or 'from pyspark.sql')

CODE REQUIREMENTS:
- Use PySpark DataFrame API (filter, select, withColumn, groupBy, agg, etc.)
- Always use f.
- Assume SparkSession exists as 'spark'
- Assume input DataFrame exists as 'df'
- Use pyspark.sql.functions for operations (import as f)
- Handle date/time operations with PySpark functions
- Use column expressions properly (f.col() or df.column)
- Include necessary imports only if essential

QUALITY STANDARDS:
- Produce syntactically correct PySpark code
- Use efficient PySpark patterns (avoid collect() unless necessary)
- Handle edge cases (nulls, type conversions)
- Use proper column referencing
- Leverage catalyst optimizer-friendly operations
- Use appropriate data types and casting

COMMON PATTERNS:
- Filtering: df.filter(f.col("age") > 65)
- Selection: df.select("id", "diagnosis")
- Grouping: df.groupBy("diagnosis").count()
- New columns: df.withColumn("new_col", f.col("old_col") * 2)
- Aggregations: df.groupBy("col").agg(F.count("*").alias("count"))
- Joins: df1.join(df2, on="key", how="inner")
- Window functions: f.row_number().over(Window.partitionBy("col").orderBy("date"))""",
                    messages=[
                        {
                            "role": "user",
                            "content": f"""Generate PySpark code for these instructions:

{instructions}

Remember: Output ONLY raw Python code. No explanations. No markdown. Start with the code itself."""
                        }
                    ]
                )
                
                # Extract code from response
                code = message.content[0].text.strip()
                
                # Clean up any markdown formatting
                code = code.replace("```python", "").replace("```", "").strip()
                
                st.subheader("Generated Code")
                st.code(code, language="python")
                
            except Exception as e:
                st.error(f"Error generating code: {str(e)}")
