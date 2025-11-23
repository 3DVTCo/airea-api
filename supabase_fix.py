# Add this to the top of airea_api_server_v2_clean.py after the imports

def search_knowledge_base_NEW(query: str, limit: int = 500):
    """Simple Supabase search"""
    from supabase import create_client
    
    with open('/Users/tedfinkleman/Downloads/lvhr-airea-full/.env', 'r') as f:
        env = f.read()
    url = env.split('VITE_SUPABASE_URL=')[1].split('\n')[0].strip().strip('"')
    key = env.split('VITE_SUPABASE_ANON_KEY=')[1].split('\n')[0].strip().strip('"')
    supabase = create_client(url, key)
    
    if "how many documents" in query.lower():
        result = supabase.table('airea_knowledge').select('id', count='exact').execute()
        return [{
            'content': f"I have {result.count} documents in Supabase.",
            'metadata': {'source': 'supabase'},
            'collection': 'system_state',
            'relevance': 100
        }]
    
    results = supabase.table('airea_knowledge').select('*').ilike('content', f'%{query}%').limit(limit).execute()
    return [{'content': doc['content'], 'metadata': doc.get('metadata', {}), 
             'collection': doc.get('collection_name', 'unknown'), 'relevance': 1} 
            for doc in results.data]
