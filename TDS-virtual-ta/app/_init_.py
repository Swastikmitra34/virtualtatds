from app.rag import embedder, index, metadata


import openai


def questions(question: str, top_k: int = 5):
    question_embedding = embedder.encode([question])
    D, I = index.search(question_embedding, top_k)

    context_chunks = [metadata[i]["text"] for i in I[0]]
    urls = [{"url": metadata[i].get("url", ""), "text": metadata[i].get("title", "Reference")} for i in I[0]]

    context = "\n\n".join(context_chunks)

    prompt = f"""You are a helpful virtual TA for the TDS course. Use the context below to answer the question:

Context:
{context}

Question:
{question}

Answer:"""

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    return {
        "answer": response["choices"][0]["message"]["content"].strip(),
        "links": urls
    }