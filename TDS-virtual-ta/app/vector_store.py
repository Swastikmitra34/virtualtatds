import pickle
import faiss
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer("all-MiniLM-L6-v2")

with open("data/embeddings.pkl", "rb") as f:
    index, metadata = pickle.load(f)

