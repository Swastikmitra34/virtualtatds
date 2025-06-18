from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import os
from dotenv import load_dotenv
import openai
import base64

# Make sure this import is correct. Update path if needed.
from app.vector import search_similar

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# FastAPI app
app = FastAPI()

# Request format
class Query(BaseModel):
    question: str
    image: Optional[str] = None  # Base64 encoded image (optional)

# API endpoint
@app.post("/api/")
async def get_answer(query: Query):
    question = query.question
    image = query.image

    # OPTIONAL: process the image here if needed using base64
    # Example: decoded_image = base64.b64decode(image)

    # Step 1: Search similar Discourse/course content
    references = search_similar(question)  # Your vector search function
    # `references` should return list of (url, text) tuples

    # Step 2: (Mock) Answer generation — Replace this with actual OpenAI call
    answer = f"Based on course discussion, the best approach for: '{question}' is to use GPT-3.5 Turbo."

    # Step 3: Format response
    return {
        "answer": answer,
        "links": [
            {"url": url, "text": text}
            for url, text in references[:2]  # Return top 2 references
        ]
    }

