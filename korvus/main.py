from korvus import Collection, Pipeline
import asyncio

collection = Collection("korvus-demo-v0")
pipeline = Pipeline(
    "v1",
    {
        "text": {
            "splitter": {"model": "recursive_character"},
            "semantic_search": {"model": "Alibaba-NLP/gte-base-en-v1.5", "parameters": {"trust_remote_code": "True"}},
        }
    },
)

async def add_pipeline():
    await collection.add_pipeline(pipeline)


async def upsert_documents():
    documents = [
        {"id": "1", "text": "Korvus is incredibly fast and easy to use."},
        {"id": "2", "text": "Tomatoes are incredible on burgers."},
    ]
    await collection.upsert_documents(documents)

async def rag():
    query = "Is Korvus fast?"
    print(f"Querying for response to: {query}")
    results = await collection.rag(
        {
            "CONTEXT": {
                "vector_search": {
                    "query": {
                        "fields": {"text": {"query": query}},
                    },
                    "document": {"keys": ["id"]},
                    "limit": 1,
                },
                "aggregate": {"join": "\n"},
            },
            "chat": {
                "model": "meta-llama/Meta-Llama-3-8B-Instruct",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a friendly and helpful chatbot",
                    },
                    {
                        "role": "user",
                        "content": f"Given the context\n:{{CONTEXT}}\nAnswer the question: {query}",
                    },
                ],
                "max_tokens": 100,
            },
        },
        pipeline,
    )
    print(results)

if __name__ == "__main__":
    asyncio.run(add_pipeline())
    asyncio.run(upsert_documents())    
    asyncio.run(rag())    



