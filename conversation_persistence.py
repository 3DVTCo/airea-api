from datetime import datetime
import uuid

def save_conversation(user_message: str, airea_response: str, client):
    """Save conversation to ChromaDB"""
    try:
        conv_collection = client.get_or_create_collection("airea_conversations")
        
        conversation_doc = f"""User: {user_message}
AIREA: {airea_response}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        metadata = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "topic": "ongoing_conversation",
            "user": "ted"
        }
        
        conv_collection.add(
            documents=[conversation_doc],
            metadatas=[metadata],
            ids=[f"conv_{datetime.now().timestamp()}_{uuid.uuid4()}"]
        )
        print(f"Saved conversation to collection")
    except Exception as e:
        print(f"Failed to save conversation: {e}")

def get_recent_conversations(client, limit: int = 5) -> str:
    """Get recent conversations for context"""
    try:
        conv_collection = client.get_collection("airea_conversations")
        results = conv_collection.get()
        
        # Filter for ONLY new conversation format (starts with "User:")
        conversations = []
        for doc in results['documents']:
            if doc.startswith("User:"):
                conversations.append(doc)
        
        # Return the most recent ones
        return "\n\n".join(conversations[-limit:])
        
    except Exception as e:
        print(f"Failed to get conversations: {e}")
        return ""
