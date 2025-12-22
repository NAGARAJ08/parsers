import json
import os
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import StructuredOutputParser, ResponseSchema
from langchain_community.llms import Ollama
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
INPUT_FILE = "code_nodes_for_summary.json"
OUTPUT_FILE = "code_nodes_enhanced.json"

# Check for API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_LOCAL_LLM = OPENAI_API_KEY is None  # Use local LLM if no API key is set

# Initialize LLM
if USE_LOCAL_LLM:
    llm = Ollama(model="gemma:2b")  # Local model
else:
    llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY)  # OpenAI ChatGPT

# Define the response schema
response_schemas = [
    ResponseSchema(name="summary", description="A concise and meaningful summary of the code element")
]
output_parser = StructuredOutputParser(response_schemas=response_schemas)

# Define the prompt template
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", "You are an expert software documentation writer."),
        ("system", output_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")),
        ("user", "Generate a summary for the following code element:\n\n"
                 "Name: {name}\n"
                 "Type: {type}\n"
                 "Service: {service}\n"
                 "Code Snippet:\n{snippet}\n\n"
                 "Summary:")
    ]
)

# Chain the prompt and LLM
chain = prompt | llm | output_parser

def generate_summaries(input_file, output_file):
    """Generate summaries for CodeNodes."""
    # Load CodeNodes
    with open(input_file, "r", encoding="utf-8") as f:
        code_nodes = json.load(f)

    # Process each CodeNode
    for node in code_nodes:
        if not node.get("new_summary"):
            try:
                # Generate summary
                result = chain.invoke({
                    "name": node["name"],
                    "type": node["type"],
                    "service": node["serviceName"],
                    "snippet": node["snippet"]
                })
                node["new_summary"] = result["summary"]
                print(f"Generated summary for {node['name']} ({node['type']})")
            except Exception as e:
                print(f"Error generating summary for {node['name']}: {e}")

    # Save enhanced CodeNodes
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(code_nodes, f, indent=4)

    print(f"\n✅ Summaries saved to {output_file}")

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found!")
    else:
        print("Starting summary generation...")
        generate_summaries(INPUT_FILE, OUTPUT_FILE)