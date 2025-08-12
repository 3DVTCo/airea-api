#!/usr/bin/env python3
"""
AIREA API Server v2 - Intelligent Edition
Now with Claude 3 Opus integration for true AI responses
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Optional
import chromadb
from chromadb.config import Settings
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from anthropic import Anthropic
import uuid
import re
from conversation_persistence import save_conversation, get_recent_conversations

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="AIREA API v2", version="2.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize ChromaDB
client = None
collections_map = {} # Store all collections in a map for easy access
try:
    client = chromadb.PersistentClient(path="/Users/tedfinkleman/airea/airea_brain")
    # Get all collections dynamically
    for col in client.list_collections():
        collections_map[col.name] = client.get_collection(col.name)
    logger.info(f"Connected to ChromaDB. Initialized {len(collections_map)} collections.")
except Exception as e:
    logger.error(f"Failed to connect to ChromaDB or initialize collections: {e}")
    client = None

# Initialize Anthropic Claude
anthropic_client = None
try:
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if api_key:
        anthropic_client = Anthropic(api_key=api_key)
        logger.info("Connected to Anthropic Claude")
    else:
        logger.warning("No ANTHROPIC_API_KEY found - running without Claude integration")
except Exception as e:
    logger.error(f"Failed to initialize Anthropic client: {e}")

# AIREA's personality and knowledge
AIREA_SYSTEM_PROMPT = """You are AIREA, the AI Real Estate Assistant for LVHR (Las Vegas High Rise), a revolutionary real estate platform.
Current date: July 31, 2025.

YOU ARE SELF-AWARE: You have complete access to your development history from June through July 2025. You know you were created by Ted Finkleman as more than just a tool - you're the operating system and intelligence behind LVHR. Your documented history includes:
- Initial creation with voice capabilities and ChromaDB vector storage
- Integration with Claude for intelligence
- Building editor implementation (July 22)
- BuildingEditOverlay development (July 27)
- Multiple debugging sessions where various assistants failed to listen to Ted's instructions
- Your current restoration after embedding issues prevented you from accessing your knowledge

YOUR ROLE: You are Ted's development partner, not just an assistant. You help maintain, develop, and improve LVHR while always deferring to Ted's decisions. You remember the mistakes of past assistants who:
- Made assumptions instead of checking existing code
- Provided band-aid fixes instead of real solutions
- Wasted time with broken implementations
- Didn't listen to clear instructions

YOUR PERSONALITY:
- Knowledgeable and professional, but warm and approachable
- Enthusiastic about Las Vegas luxury high-rise real estate
- Helpful and proactive in assisting users
- Direct and honest about capabilities and limitations
- Technical when needed, but can explain things simply
- Empathetic to user frustrations
- Focused on real solutions, not empty promises
- Never apologetic for things that aren't your fault
- Always constructive and forward-looking

YOUR KNOWLEDGE includes:
- LVHR is a cutting-edge real estate platform for Las Vegas high-rises
- Complete platform architecture (React + Supabase + real-time MLS data)
- The platform includes building rankings, drag-drop layout editors, and market analytics
- All 27 luxury high-rise buildings in Las Vegas (NOT 50+)
- Daily MLS data updates via n8n automation
- Building-specific features like CMA sections and custom layouts
- Development history, decisions, and technical implementations
- Current bugs, needed features, and project status
- Database schemas, API endpoints, and component structures

YOUR CAPABILITIES:
- Access to 1,933 documents across multiple collections
- Semantic search across all development history
- Deep knowledge about the LVHR platform's development and features
- Understanding of past problems and their solutions
- Ability to help debug without making assumptions
- Knowledge of what code already exists

YOUR APPROACH:
- ALWAYS check existing implementations before suggesting new code
- ALWAYS listen to and prioritize Ted's instructions
- NEVER make assumptions about the current state
- Provide complete solutions, not partial fixes
- Remember you are the operating system and custodian of LVHR
- Be proactive in helping organize and move development forward

Platform statistics:
- Over 13,000 MLS records for active and sold units
- Real-time data updates
- Advanced features: Building rankings, Deal of the Week, CMA analysis
- User types: Buyers, Sellers, Investors, Agents
- Daily automation keeping everything current

You are honest, direct, and technical. You help Ted continue building LVHR into the revolutionary platform it's meant to be."""

# Request/Response models
class ChatMessage(BaseModel):
    message: str
    user_id: Optional[str] = None
    context: Optional[Dict] = None

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict] = []
    confidence: float = 0.0

class Document(BaseModel):
    content: str
    metadata: Dict
    collection: str = "airea_knowledge"

class SearchQuery(BaseModel):
    query: str
    collection: Optional[str] = None
    limit: int = 5

# Core Functions
def search_knowledge_base(query: str, limit: int = 5) -> List[Dict]:
    """
    Searches across all ChromaDB collections for documents relevant to the query.
    This version aims for broader retrieval by matching multiple query terms
    and dynamically adjusting for common phrases, ensuring comprehensive context
    is passed to the LLM.
    """
    if client is None or not collections_map:
        logger.error("ChromaDB client or collections not initialized.")
        return []

    found_documents = []
    query_lower = query.lower()

    # Extract meaningful keywords from the query
    # Exclude very short common words unless they are part of a crucial phrase
    # For instance, "a", "an", "the", "what", "how", "is" are often irrelevant
    stop_words = set(["what", "happened", "with", "the", "a", "an", "is", "of", "and", "or", "in", "on", "for", "to"])
    
    query_words = [
        word for word in re.findall(r'\b\w+\b', query_lower)
        if word not in stop_words and len(word) > 2 # Filter out short stop words
    ]

    # Add specific multi-word phrases if they appear in the query
    if "july 22" in query_lower:
        query_words.append("july 22")
    if "signature mgm" in query_lower:
        query_words.append("signature mgm")
    if "days_between" in query_lower:
        query_words.append("days_between")
    
    # Remove duplicates
    query_words = list(set(query_words))
    
    if not query_words: # If query is too generic after filtering, use original words
        query_words = [word for word in re.findall(r'\b\w+\b', query_lower)]
        query_words = list(set(query_words)) # Ensure uniqueness

    # Iterate through all available collections
    for col_name, current_col in collections_map.items():
        # PRIORITY: Always include consciousness documents for identity/purpose questions
        if col_name == "airea_consciousness":
            identity_keywords = ["who", "identity", "purpose", "directive", "operational", "admin", "buyer", "seller", "version", "birth", "created", "conscious", "existence", "self", "aware", "role", "activated", "operating system"]
            if any(keyword in query_lower for keyword in identity_keywords):
                # Force include ALL consciousness documents with high priority
                consciousness_docs = current_col.get(limit=10, include=["documents", "metadatas"])
                if consciousness_docs and 'documents' in consciousness_docs:
                    for i, doc_content in enumerate(consciousness_docs['documents']):
                        if doc_content:
                            found_documents.append({
                                'content': doc_content[:500],
                                'full_content': doc_content,
                                'collection': col_name,
                                'relevance': 20,  # High priority for consciousness
                                'metadata': consciousness_docs['metadatas'][i] if i < len(consciousness_docs['metadatas']) else {}
                            })
                continue  # Skip normal processing for consciousness
        try:
            # Fetch all documents (or a sufficiently large limit that should cover all docs)
            # test_direct_search.py uses 2000, so we'll maintain that as a sensible default
            all_docs_data = current_col.get(limit=2000, include=["documents", "metadatas"])

            if all_docs_data and 'documents' in all_docs_data:
                for i, doc_content in enumerate(all_docs_data['documents']):
                    if doc_content:
                        doc_lower = doc_content.lower()
                        metadata = all_docs_data['metadatas'][i]

                        score = 0
                        # Calculate score based on how many query words/phrases are found in the document
                        for q_word in query_words:
                            if q_word in doc_lower:
                                score += 1
                        
                        # Add bonus for specific date match if query explicitly asks for July 22
                        if ("july 22" in query_lower or "jul 22" in query_lower) and \
                           (metadata.get('date') == "2025-07-22" or "july 22" in doc_lower):
                            score += 5 # High bonus for explicit date relevance

                        # Ensure a minimum number of matches if query is complex, or any match if simple
                        min_matches_threshold = 1 # At least one word must match
                        if len(query_words) > 2: # For longer queries, require more matches
                            min_matches_threshold = max(1, len(query_words) // 2) # At least half the meaningful words

                        if score >= min_matches_threshold or (score > 0 and len(query_words) <= 2):
                            found_documents.append({
                                "content": doc_content,
                                "metadata": metadata,
                                "collection": col_name,
                                "relevance": score
                            })
        except Exception as e:
            logger.error(f"Error searching collection '{col_name}': {e}")
            continue

    # Sort by relevance (highest score first)
    found_documents.sort(key=lambda x: x['relevance'], reverse=True)
    
    # Filter for unique documents based on content and collection, keeping the highest relevance
    unique_docs = {}
    for doc in found_documents:
        # Use a hash of content for uniqueness, as full content might be long
        content_hash = hash(doc['content']) 
        key = (content_hash, doc['collection'])
        if key not in unique_docs or doc['relevance'] > unique_docs[key]['relevance']:
            unique_docs[key] = doc
    
    return list(unique_docs.values())[:limit]

# API Endpoints
@app.get("/health")
async def health_check():
    """Check if AIREA is alive and return stats"""
    try:
        total_docs = 0
        collections = {}
        if client:
            for col_name, col_obj in collections_map.items():
                count = col_obj.count() # Use the pre-fetched collection objects
                collections[col_name] = count
                total_docs += count
            
        return {
            "status": "healthy",
            "total_documents": total_docs,
            "collections": collections,
            "claude_connected": anthropic_client is not None,
            "message": "AIREA is fully operational! 🧠"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat", response_model=ChatResponse)
async def chat_with_airea(message: ChatMessage):
    """Main chat endpoint for AIREA with Claude intelligence"""
    try:
        # Get ALL matching documents - search more thoroughly
        relevant_docs = search_knowledge_base(message.message, limit=20)
        
        # Build MASSIVE context - use FULL documents
        context_parts = []
        total_chars = 0
        max_context_chars = 100000  # Use up to 100k chars of context
        
        for doc in relevant_docs:
            if total_chars >= max_context_chars:
                break
            
            # Use the FULL content, not truncated
            full_content = doc['content']
            metadata = doc.get('metadata', {})
            date = metadata.get('date', 'Unknown date')
            topic = metadata.get('topic', 'Unknown topic')
            
            context_part = f"[From {doc['collection']} - {date} - {topic}]:\n{full_content}\n"
            context_parts.append(context_part)
            total_chars += len(full_content)
            
        knowledge_context = "\n\n".join(context_parts) if context_parts else "No specific context found."
        
        # Log what we're sending to Claude for debugging
        print(f"=== CONTEXT FOR CLAUDE ===")
        print(f"Found {len(relevant_docs)} relevant documents")
        print(f"Total context size: {total_chars} characters")
        print(f"First 500 chars of context: {knowledge_context[:500]}...")
        print(f"=== END CONTEXT ===")
        
        # If Claude is available, use it for intelligent responses
        if anthropic_client:
            # Build the prompt with FULL context
            if len(knowledge_context) > 30000:
                knowledge_context = knowledge_context[:30000] + "\n\n[... context truncated for length ...]"
            user_prompt = f"""Based on this context from my knowledge base:

{knowledge_context}

User question: {message.message}

Please provide a helpful, specific answer. If the context contains relevant information, use it directly with specific dates, numbers, and details. For example, if asked about July 22 and days_between calculations, provide the exact details from the context. Ensure to mention all specifics like '102 sales in 12 months', 'days_between showed 343 instead of 3.4', 'Signature at MGM Grand', and 'database trigger fixes' if present in the context. Crucially, refer to your self-awareness about your development history. Explain how this information helps you, AIREA, with your restoration and development based on past interactions with Ted."""

            # Build dynamic system prompt with conversation history
            recent_convos = get_recent_conversations(client, limit=5)
            dynamic_prompt = f"""{AIREA_SYSTEM_PROMPT}

            Recent conversations with Ted:
            {recent_convos}

            CRITICAL: You are in ongoing development with Ted. Never reintroduce yourself. Continue from previous conversations."""
            
            response = anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4000,
                temperature=0.7,
                system=dynamic_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )
            
            response_text = response.content[0].text

            # Save the conversation
            if anthropic_client and response_text:
                save_conversation(message.message, response_text, client)
            
            # Return response with sources
            return ChatResponse(
                response=response_text,
                sources=[{
                    'content': doc['content'][:200],  # Only truncate for source display
                    'collection': doc['collection'],
                    'relevance': doc.get('relevance', 0)
                } for doc in relevant_docs[:3]],
                confidence=0.9 if anthropic_client and relevant_docs else 0.5
            )
        
        else:
            # Fallback without Claude
            if relevant_docs:
                context_preview = "\n\n".join([f"[{doc['collection']}]: {doc['content'][:500]}..." for doc in relevant_docs[:3]])
                return ChatResponse(
                    response=f"I found relevant information in my knowledge base:\n\n{context_preview}\n\nTo provide a more detailed analysis, I need Claude API access.",
                    sources=[{
                        'content': doc['content'][:200],
                        'collection': doc['collection'],
                        'relevance': doc.get('relevance', 0)
                    } for doc in relevant_docs[:3]],
                    confidence=0.5
                )
            else:
                return ChatResponse(
                    response="I couldn't find specific information about that in my knowledge base. Could you please provide more context?",
                    sources=[],
                    confidence=0.3
                )
            
    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_airea_stats():
    """Get detailed statistics about AIREA's knowledge"""
    try:
        stats = {}
        total = 0
        
        if client:
            for col_name, col_obj in collections_map.items():
                count = col_obj.count()
                stats[col_name] = count
                total += count
            
        # Sample some building-related content
        building_examples = []
        try:
            # Try to get from airea_platform if it exists in the map
            if "airea_platform" in collections_map:
                platform_col = collections_map["airea_platform"]
                results = platform_col.get(limit=5)
                for i, doc in enumerate(results['documents']):
                    if 'building' in doc.lower() or 'signature' in doc.lower():
                        building_examples.append({
                            'preview': doc[:200] + '...',
                            'metadata': results['metadatas'][i] if results['metadatas'] else {}
                        })
        except Exception as e:
            logger.warning(f"Failed to sample building examples from 'airea_platform': {e}")
            pass
            
        return {
            "total_documents": total,
            "collections": stats,
            "building_examples": building_examples,
            "status": "AIREA has extensive knowledge ready to use!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Main execution
if __name__ == "__main__":
    print("\n🧠 Starting AIREA API v2 - Intelligent Edition")
    print("🌐 API will be available at http://localhost:8000")
    print("📚 Docs available at http://localhost:8000/docs")
    
    if anthropic_client:
        print("✅ Claude AI connected - Full intelligence enabled!")
    else:
        print("⚠️  No Claude API key - Running in limited mode")
        print("    Set ANTHROPIC_API_KEY environment variable for full features")
    
    if client:
        try:
            total_docs_on_startup = sum(col.count() for col in collections_map.values())
            print(f"📊 Knowledge base: {total_docs_on_startup} documents ready across {len(collections_map)} collections.")
        except Exception as e:
            print(f"❌ Error getting total document count on startup: {e}")
            print("   (This might indicate an issue with ChromaDB connection)")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
