from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from vector_store import search_similar
import openai
import base64

openai.api_key = "YOUR_OPENAI_API_KEY"

class Query(BaseModel):
    question: str
    image: Optional[str] = None

app = FastAPI()

@app.post("/api/")
def answer_question(query: Query):
    question = query.question
    links = search_similar(question)
    context = "\n".join([link['text'] for link in links])

    prompt = f"You are a TDS Teaching Assistant. Use this context to answer:\n\n{context}\n\nStudent Question: {question}\n\nAnswer concisely and clearly."
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    answer = response.choices[0].message['content']
    return {
        "answer": answer,
        "links": links
    }

