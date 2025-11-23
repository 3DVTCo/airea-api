from supabase import create_client

with open('/Users/tedfinkleman/Downloads/lvhr-airea-full/.env', 'r') as f:
    env = f.read()
url = env.split('VITE_SUPABASE_URL=')[1].split('\n')[0].strip().strip('"')
key = env.split('VITE_SUPABASE_ANON_KEY=')[1].split('\n')[0].strip().strip('"')
supabase = create_client(url, key)

# Test the search
results = supabase.table('airea_knowledge').select('*').ilike('content', '%platform%').limit(5).execute()
print(f"Found {len(results.data)} documents")
for doc in results.data[:2]:
    print(f"- {doc['content'][:100]}...")
