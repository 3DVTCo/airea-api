# WORKING STATE - November 11, 2025

## ✅ Confirmed Working:
- AIREA responds with correct date (November 11, 2025)
- Document count: 9,550 (dynamic from Supabase)
- Search across November 2025 knowledge base
- Security fixes searchable and retrievable
- Chat bubble + window both displaying correctly
- Conversation persistence to Supabase airea_conversations table
- UltraLux building list integrated
- Model: claude-sonnet-4-20250514

## Key Files:
- airea_api_server_v2.py (dynamic date/count, build_system_prompt function)
- conversation_persistence.py (Supabase integration with correct env vars)

## Deployment:
- Render Status: Live ✅
- Last successful deploy: November 11, 2025 ~12:20 PM
- Auto-deploy: Enabled from main branch

## Environment Variables (Render):
- ANTHROPIC_API_KEY: Set ✅
- SUPABASE_URL: Set ✅
- SUPABASE_KEY: Set ✅

## GitHub Access:
- Token regenerated: November 11, 2025
- Expires: December 11, 2025
- Saved in git remote URL

## Restore Instructions:
If anything breaks, restore this working state:
```bash
git checkout v1.0-working-nov11
git push origin main --force
```
Then manually trigger Render redeploy.
