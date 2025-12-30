#!/usr/bin/env python3
"""
AIREA Data Tools - MCP Server for LVHR Platform
================================================
Provides Claude Desktop with live Supabase query capabilities.

15 TOOLS AVAILABLE:

DATA QUERY (8):
- query_active_listings: Get active listings by building or overall
- query_building_rankings: Get building rankings with score_v2
- query_market_cma: Get CMA data for market analysis
- query_deal_of_week: Get current deal of the week data
- search_airea_knowledge: Search AIREA's knowledge base
- query_sales_history: Get historical sales data
- get_building_list: Get all building names
- query_penthouse_listings: Get active penthouses

PROSPECTING (2):
- get_hot_leads: Properties from hot_list (highest probability sellers)
- query_stale_listings: Expired/withdrawn listings (frustrated sellers)

CONTENT (2):
- explain_deal_selection: Explain Deal of Week selection (NO "discount" language)
- generate_market_report: Monthly/quarterly/yearly market reports

TEAM TASKS (3):
- create_team_task: Create task in Team Workspace Kanban
- get_team_tasks: Get tasks from Kanban board
- update_task_status: Update task status/priority

VERIFIED TABLES USED:
- lvhr_master (source of truth)
- building_rankings / midrise_rankings
- market_cma / market_cma_overall / market_cma_above_1m / market_cma_below_1m
- deal_of_week_overall / deal_of_week_building
- sales
- airea_knowledge
- hot_list / stale_listings_prospecting
- team_tasks / user_profiles

Usage:
  python airea_data_tools.py

MCP Configuration (claude_desktop_config.json):
{
  "mcpServers": {
    "airea-data": {
      "command": "python3",
      "args": ["/path/to/airea_data_tools.py"],
      "env": {
        "SUPABASE_URL": "your-url",
        "SUPABASE_KEY": "your-key"
      }
    }
  }
}
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

# MCP SDK imports
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
except ImportError:
    print("ERROR: MCP SDK not installed. Run: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Supabase import
try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: Supabase not installed. Run: pip install supabase", file=sys.stderr)
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("airea-data-tools")

# =============================================================================
# VERIFIED REFERENCE DATA (from PRD v5.0)
# =============================================================================

# Active listing status codes - VERIFIED
ACTIVE_STATUS_CODES = ["A-ER", "A-EA", "CSL"]

# Excluded status codes (under contract)
EXCLUDED_STATUS_CODES = ["COS", "UCNS", "UCS"]

# Key column names (require quotes due to spaces)
COLUMN_NAMES = {
    "tower_name": '"Tower Name"',
    "list_price": '"List Price"',
    "lp_sqft": '"LP/SqFt"',
    "beds_total": '"Beds Total"',
    "stat": '"Stat"',
    "close_price": '"Close Price"',
    "actual_close_date": '"Actual Close Date"',
    "dom": '"DOM"',
}

# Building counts - VERIFIED
HIGHRISE_COUNT = 27
MIDRISE_COUNT = 6

# =============================================================================
# SUPABASE CLIENT
# =============================================================================

def get_supabase_client() -> Client:
    """Get Supabase client from environment variables."""
    url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables required")
    
    return create_client(url, key)

# =============================================================================
# TOOL DEFINITIONS
# =============================================================================

TOOLS = [
    # ==========================================================================
    # DATA QUERY TOOLS (8)
    # ==========================================================================
    Tool(
        name="query_active_listings",
        description="""Query active listings from lvhr_master table.
        
        Status codes for active: A-ER, A-EA, CSL
        Can filter by building name, price range, bedrooms.
        Returns: MLS#, address, price, sqft, beds, baths, DOM, status.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Filter by building name (e.g., 'Veer Towers', 'Waldorf Astoria')"
                },
                "min_price": {
                    "type": "number",
                    "description": "Minimum list price"
                },
                "max_price": {
                    "type": "number",
                    "description": "Maximum list price"
                },
                "bedrooms": {
                    "type": "integer",
                    "description": "Filter by number of bedrooms"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results to return (default: 20)",
                    "default": 20
                }
            }
        }
    ),
    Tool(
        name="query_building_rankings",
        description="""Query building rankings from building_rankings table.
        
        Returns rankings sorted by score_v2 (higher = better).
        Includes: rank, building name, score, active listings, avg price, avg ppsf.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Get ranking for specific building"
                },
                "top_n": {
                    "type": "integer",
                    "description": "Return top N buildings (default: 10)",
                    "default": 10
                },
                "include_midrise": {
                    "type": "boolean",
                    "description": "Include midrise buildings (queries midrise_rankings)",
                    "default": False
                }
            }
        }
    ),
    Tool(
        name="query_market_cma",
        description="""Query market CMA (Comparative Market Analysis) data.
        
        Returns: building-level market stats including avg price, avg ppsf, 
        active count, sold count, price trends.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Filter by building name"
                },
                "segment": {
                    "type": "string",
                    "enum": ["all", "above_1m", "below_1m"],
                    "description": "Price segment (default: all)",
                    "default": "all"
                }
            }
        }
    ),
    Tool(
        name="query_deal_of_week",
        description="""Query current Deal of the Week data.
        
        Returns: featured deal with DealScore, narrative, peer comparison data.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Get deal for specific building"
                },
                "include_backup": {
                    "type": "boolean",
                    "description": "Include backup deals",
                    "default": False
                }
            }
        }
    ),
    Tool(
        name="search_airea_knowledge",
        description="""Search AIREA's knowledge base (airea_knowledge table).
        
        Searches document content and metadata for relevant information.
        Returns: matching documents with content snippets.""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 5)",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="query_sales_history",
        description="""Query historical sales data from sales table.
        
        Returns: closed sales with price, date, ppsf, building info.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Filter by building name"
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 50)",
                    "default": 50
                }
            }
        }
    ),
    Tool(
        name="get_building_list",
        description="""Get list of all buildings in the database.
        
        Returns: all 27 high-rise and 6 mid-rise building names.""",
        inputSchema={
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["highrise", "midrise", "all"],
                    "description": "Building type filter",
                    "default": "all"
                }
            }
        }
    ),
    Tool(
        name="query_penthouse_listings",
        description="""Query penthouse listings from lvhr_master.
        
        Returns: active penthouse listings with full details.""",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 20)",
                    "default": 20
                }
            }
        }
    ),
    
    # ==========================================================================
    # PROSPECTING TOOLS (2)
    # ==========================================================================
    Tool(
        name="get_hot_leads",
        description="""Get properties from hot_list - highest probability sellers.
        
        Joins hot_list (MLS numbers) with lvhr_master for full property details.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Filter by building name"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 20)",
                    "default": 20
                }
            }
        }
    ),
    Tool(
        name="query_stale_listings",
        description="""Get expired and withdrawn listings - frustrated sellers.
        
        Returns listings that failed to sell with days on market,
        original price, and owner hold time for second-chance opportunities.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Filter by building name"
                },
                "months_back": {
                    "type": "integer",
                    "description": "How far back to look (default: 12 months)",
                    "default": 12
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 20)",
                    "default": 20
                }
            }
        }
    ),
    
    # ==========================================================================
    # CONTENT GENERATION TOOLS (2)
    # ==========================================================================
    Tool(
        name="explain_deal_selection",
        description="""Explain why a unit was selected as Deal of the Week.
        
        Provides narrative reasoning based on DealScore formula.
        NEVER uses words like 'discount' or 'savings'. 
        Focuses on: value positioning, market opportunity, comparative advantage.""",
        inputSchema={
            "type": "object",
            "properties": {
                "building_name": {
                    "type": "string",
                    "description": "Building name"
                },
                "mls_number": {
                    "type": "string",
                    "description": "MLS number of the deal (optional - uses current deal if not provided)"
                }
            },
            "required": ["building_name"]
        }
    ),
    Tool(
        name="generate_market_report",
        description="""Generate market report comparing time periods.
        
        Creates monthly, quarterly, or yearly reports for the whole market
        or specific buildings. Includes sales volume, price trends, DOM,
        absorption rate, and year-over-year comparisons.""",
        inputSchema={
            "type": "object",
            "properties": {
                "report_type": {
                    "type": "string",
                    "enum": ["monthly", "quarterly", "yearly"],
                    "description": "Report period type"
                },
                "building_name": {
                    "type": "string",
                    "description": "Specific building (omit for market-wide report)"
                },
                "year": {
                    "type": "integer",
                    "description": "Year for report (default: current year)",
                    "default": 2025
                },
                "compare_to_year": {
                    "type": "integer",
                    "description": "Year to compare against (default: previous year)",
                    "default": 2024
                }
            },
            "required": ["report_type"]
        }
    ),
    
    # ==========================================================================
    # TEAM TASK TOOLS (3)
    # ==========================================================================
    Tool(
        name="create_team_task",
        description="""Create a task in the Team Workspace Kanban board.
        
        Use for adding new tasks, assignments, and deadlines.
        Status: todo | in_progress | done
        Priority: low | medium | high""",
        inputSchema={
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Task title (required)"
                },
                "description": {
                    "type": "string",
                    "description": "Task details"
                },
                "status": {
                    "type": "string",
                    "enum": ["todo", "in_progress", "done"],
                    "description": "Task status (default: todo)",
                    "default": "todo"
                },
                "priority": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": "Task priority (default: medium)",
                    "default": "medium"
                },
                "assigned_to_name": {
                    "type": "string",
                    "description": "Team member name (e.g., Kayren, Enrico, Ted)"
                },
                "due_date": {
                    "type": "string",
                    "description": "Due date in YYYY-MM-DD format"
                }
            },
            "required": ["title"]
        }
    ),
    Tool(
        name="get_team_tasks",
        description="""Get tasks from the Team Workspace Kanban board.
        
        Filter by status or priority. Returns task list with details.""",
        inputSchema={
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["todo", "in_progress", "done"],
                    "description": "Filter by status"
                },
                "priority": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": "Filter by priority"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max tasks to return (default: 20)",
                    "default": 20
                }
            }
        }
    ),
    Tool(
        name="update_task_status",
        description="""Update a task's status or priority in Team Workspace.
        
        Move tasks between columns or change priority.""",
        inputSchema={
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "Task UUID (or use task_title)"
                },
                "task_title": {
                    "type": "string",
                    "description": "Search by title (or use task_id)"
                },
                "new_status": {
                    "type": "string",
                    "enum": ["todo", "in_progress", "done"],
                    "description": "New status"
                },
                "new_priority": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": "New priority"
                }
            }
        }
    ),
]

# =============================================================================
# TOOL IMPLEMENTATIONS
# =============================================================================

def query_active_listings(
    building_name: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    bedrooms: Optional[int] = None,
    limit: int = 20
) -> dict:
    """Query active listings from lvhr_master."""
    try:
        supabase = get_supabase_client()
        
        # Build query - select key columns
        query = supabase.table("lvhr_master").select(
            '"ML#", "Address", "Tower Name", "List Price", "LP/SqFt", '
            '"Beds Total", "Baths Total", "Approx Liv Area", "DOM", "Stat"'
        )
        
        # Filter by active status codes
        query = query.in_('"Stat"', ACTIVE_STATUS_CODES)
        
        # Apply filters
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        if min_price:
            query = query.gte('"List Price"', str(min_price))
        
        if max_price:
            query = query.lte('"List Price"', str(max_price))
        
        if bedrooms:
            query = query.eq('"Beds Total"', bedrooms)
        
        # Execute with limit
        response = query.limit(limit).execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "status_codes_used": ACTIVE_STATUS_CODES,
            "listings": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_building_rankings(
    building_name: Optional[str] = None,
    top_n: int = 10,
    include_midrise: bool = False
) -> dict:
    """Query building rankings."""
    try:
        supabase = get_supabase_client()
        results = {}
        
        # Query highrise rankings
        query = supabase.table("building_rankings").select("*")
        
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        query = query.order("score_v2", desc=True).limit(top_n)
        response = query.execute()
        
        results["highrise"] = {
            "count": len(response.data),
            "total_buildings": HIGHRISE_COUNT,
            "rankings": response.data
        }
        
        # Query midrise if requested
        if include_midrise:
            midrise_query = supabase.table("midrise_rankings").select("*")
            if building_name:
                midrise_query = midrise_query.eq('"Tower Name"', building_name)
            midrise_query = midrise_query.order("score_v2", desc=True).limit(top_n)
            midrise_response = midrise_query.execute()
            
            results["midrise"] = {
                "count": len(midrise_response.data),
                "total_buildings": MIDRISE_COUNT,
                "rankings": midrise_response.data
            }
        
        return {"success": True, **results}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_market_cma(
    building_name: Optional[str] = None,
    segment: str = "all"
) -> dict:
    """Query market CMA data."""
    try:
        supabase = get_supabase_client()
        
        # Select table based on segment
        table_name = {
            "all": "market_cma",
            "above_1m": "market_cma_above_1m",
            "below_1m": "market_cma_below_1m"
        }.get(segment, "market_cma")
        
        query = supabase.table(table_name).select("*")
        
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        response = query.execute()
        
        return {
            "success": True,
            "table": table_name,
            "count": len(response.data),
            "data": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_deal_of_week(
    building_name: Optional[str] = None,
    include_backup: bool = False
) -> dict:
    """Query Deal of the Week data."""
    try:
        supabase = get_supabase_client()
        
        if building_name:
            # Query building-specific deal
            query = supabase.table("deal_of_week_building").select("*")
            query = query.eq("building_name", building_name)
        else:
            # Query overall deal
            query = supabase.table("deal_of_week_overall").select("*")
        
        if not include_backup:
            query = query.eq("is_primary", True)
        
        response = query.execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "deals": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def search_airea_knowledge(query: str, limit: int = 5) -> dict:
    """Search AIREA's knowledge base."""
    try:
        supabase = get_supabase_client()
        
        # Text search on content
        response = supabase.table("airea_knowledge").select(
            "id, content, metadata, created_at"
        ).ilike("content", f"%{query}%").limit(limit).execute()
        
        results = []
        for doc in response.data:
            # Truncate content for display
            content = doc.get("content", "")
            snippet = content[:500] + "..." if len(content) > 500 else content
            results.append({
                "id": doc.get("id"),
                "snippet": snippet,
                "metadata": doc.get("metadata"),
                "created_at": doc.get("created_at")
            })
        
        return {
            "success": True,
            "query": query,
            "count": len(results),
            "results": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_sales_history(
    building_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50
) -> dict:
    """Query historical sales data from lvhr_master (source of truth)."""
    try:
        supabase = get_supabase_client()
        
        # Query lvhr_master directly - it has ALL sales data
        # S = Sold (first 365 days), H = Historical (day 366+)
        query = supabase.table("lvhr_master").select(
            '"ML#", "Address", "Tower Name", "Close Price", "SP/SqFt", '
            '"Beds Total", "Baths Total", "Approx Liv Area", "Actual Close Date", '
            '"Stat", "actual_close_date_parsed"'
        )
        
        # Filter for sold statuses only
        query = query.in_('"Stat"', ['S', 'H'])
        
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        # Use actual_close_date_parsed (proper DATE type) for filtering
        if start_date:
            query = query.gte("actual_close_date_parsed", start_date)
        
        if end_date:
            query = query.lte("actual_close_date_parsed", end_date)
        
        # Use actual_close_date_parsed for proper date sorting
        query = query.order("actual_close_date_parsed", desc=True).limit(limit)
        response = query.execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "source": "lvhr_master",
            "status_codes": ["S", "H"],
            "sales": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_building_list(type: str = "all") -> dict:
    """Get list of all buildings."""
    try:
        supabase = get_supabase_client()
        results = {}
        
        if type in ["all", "highrise"]:
            response = supabase.table("building_rankings").select('"Tower Name"').execute()
            results["highrise"] = {
                "count": len(response.data),
                "buildings": [r.get("Tower Name") for r in response.data]
            }
        
        if type in ["all", "midrise"]:
            response = supabase.table("midrise_rankings").select('"Tower Name"').execute()
            results["midrise"] = {
                "count": len(response.data),
                "buildings": [r.get("Tower Name") for r in response.data]
            }
        
        return {"success": True, **results}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_penthouse_listings(limit: int = 20) -> dict:
    """Query penthouse listings."""
    try:
        supabase = get_supabase_client()
        
        query = supabase.table("lvhr_master").select(
            '"ML#", "Address", "Tower Name", "List Price", "LP/SqFt", '
            '"Beds Total", "Baths Total", "Approx Liv Area", "DOM", "Stat"'
        )
        
        query = query.eq("is_penthouse", True)
        query = query.in_('"Stat"', ACTIVE_STATUS_CODES)
        query = query.order('"List Price"', desc=True).limit(limit)
        
        response = query.execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "penthouses": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# PROSPECTING TOOL IMPLEMENTATIONS
# =============================================================================

def get_hot_leads(
    building_name: Optional[str] = None,
    limit: int = 20
) -> dict:
    """Get properties from hot_list joined with lvhr_master for full details."""
    try:
        supabase = get_supabase_client()
        
        # hot_list only has ML# column - need to join with lvhr_master
        # First get the ML#s from hot_list
        hot_response = supabase.table("hot_list").select('"ML#"').execute()
        
        if not hot_response.data:
            return {
                "success": True,
                "count": 0,
                "description": "No properties in hot_list",
                "leads": []
            }
        
        # Extract MLS numbers
        mls_numbers = [row.get("ML#") for row in hot_response.data if row.get("ML#")]
        
        if not mls_numbers:
            return {
                "success": True,
                "count": 0,
                "description": "No valid MLS numbers in hot_list",
                "leads": []
            }
        
        # Query lvhr_master for full details
        query = supabase.table("lvhr_master").select(
            '"ML#", "Address", "Tower Name", "List Price", "LP/SqFt", '
            '"Beds Total", "Baths Total", "Approx Liv Area", "DOM", "Stat"'
        )
        
        query = query.in_('"ML#"', mls_numbers)
        
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        query = query.limit(limit)
        response = query.execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "description": "Properties from hot_list - highest probability sellers",
            "hot_list_total": len(mls_numbers),
            "leads": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def query_stale_listings(
    building_name: Optional[str] = None,
    months_back: int = 12,
    limit: int = 20
) -> dict:
    """Get expired and withdrawn listings from stale_listings_prospecting."""
    try:
        supabase = get_supabase_client()
        
        # Correct column names (with spaces, need quotes)
        query = supabase.table("stale_listings_prospecting").select(
            '"ML#", "Tower Name", "Unit Number", "Address", "List Price", '
            '"List Date", "DOM", "List Agent Full Name", "date_marked_stale", "previous_status"'
        )
        
        if building_name:
            query = query.eq('"Tower Name"', building_name)
        
        # Filter by date_marked_stale
        cutoff_date = (datetime.now() - timedelta(days=months_back * 30)).strftime('%Y-%m-%d')
        query = query.gte("date_marked_stale", cutoff_date)
        
        query = query.order("date_marked_stale", desc=True).limit(limit)
        response = query.execute()
        
        return {
            "success": True,
            "count": len(response.data),
            "description": "Expired/withdrawn listings - frustrated sellers, second chance opportunities",
            "months_searched": months_back,
            "listings": response.data
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# CONTENT GENERATION TOOL IMPLEMENTATIONS
# =============================================================================

def explain_deal_selection(
    building_name: str,
    mls_number: Optional[str] = None
) -> dict:
    """Explain why a unit was selected as Deal of the Week."""
    try:
        supabase = get_supabase_client()
        
        # Get deal data
        query = supabase.table("deal_of_week_building").select("*")
        query = query.eq("building_name", building_name)
        
        if mls_number:
            query = query.eq("mls_number", mls_number)
        else:
            query = query.eq("is_primary", True)
        
        response = query.execute()
        
        if not response.data:
            return {"success": False, "error": f"No deal found for {building_name}"}
        
        deal = response.data[0]
        
        # Build explanation narrative (NO "discount" language)
        explanation = {
            "building": building_name,
            "mls_number": deal.get("mls_number"),
            "deal_score": deal.get("score_metric"),
            "narrative_points": []
        }
        
        # Compare to building average
        if deal.get("building_ppsf_avg"):
            unit_ppsf = deal.get("unit_ppsf", 0)
            bldg_ppsf = deal.get("building_ppsf_avg", 0)
            if unit_ppsf < bldg_ppsf:
                diff_pct = ((bldg_ppsf - unit_ppsf) / bldg_ppsf) * 100
                explanation["narrative_points"].append(
                    f"Priced at ${unit_ppsf:.0f}/sqft - positioned {diff_pct:.1f}% below building average of ${bldg_ppsf:.0f}/sqft"
                )
        
        # Compare to peer buildings
        if deal.get("peer_ppsf_avg"):
            unit_ppsf = deal.get("unit_ppsf", 0)
            peer_ppsf = deal.get("peer_ppsf_avg", 0)
            if unit_ppsf < peer_ppsf:
                explanation["narrative_points"].append(
                    f"Competitive advantage vs peer buildings averaging ${peer_ppsf:.0f}/sqft"
                )
        
        # DOM analysis
        if deal.get("building_dom_avg"):
            unit_dom = deal.get("dom", 0)
            bldg_dom = deal.get("building_dom_avg", 0)
            if unit_dom < bldg_dom:
                explanation["narrative_points"].append(
                    f"Fresh to market at {unit_dom} days vs building average of {bldg_dom} days"
                )
        
        explanation["summary"] = (
            f"This unit represents strong value positioning within {building_name}, "
            f"offering buyers an opportunity to enter at a favorable price point "
            f"relative to both building and market comparables."
        )
        
        return {"success": True, **explanation}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def generate_market_report(
    report_type: str,
    building_name: Optional[str] = None,
    year: int = 2025,
    compare_to_year: int = 2024
) -> dict:
    """Generate market report comparing time periods."""
    try:
        supabase = get_supabase_client()
        
        # Get sales data for both years
        current_query = supabase.table("sales").select("*")
        compare_query = supabase.table("sales").select("*")
        
        if building_name:
            current_query = current_query.eq('"Tower Name"', building_name)
            compare_query = compare_query.eq('"Tower Name"', building_name)
        
        # Date filters based on report type
        # Use actual_close_date_parsed (proper DATE type) for filtering
        if report_type == "yearly":
            current_query = current_query.gte("actual_close_date_parsed", f"{year}-01-01")
            current_query = current_query.lte("actual_close_date_parsed", f"{year}-12-31")
            compare_query = compare_query.gte("actual_close_date_parsed", f"{compare_to_year}-01-01")
            compare_query = compare_query.lte("actual_close_date_parsed", f"{compare_to_year}-12-31")
        
        current_response = current_query.execute()
        compare_response = compare_query.execute()
        
        # Calculate metrics
        def calc_metrics(data):
            if not data:
                return {"count": 0, "avg_price": 0, "avg_ppsf": 0, "total_volume": 0}
            
            prices = [float(d.get("Close Price", 0) or 0) for d in data]
            ppsfs = [float(d.get("LP/SqFt", 0) or 0) for d in data]
            
            return {
                "count": len(data),
                "avg_price": sum(prices) / len(prices) if prices else 0,
                "avg_ppsf": sum(ppsfs) / len(ppsfs) if ppsfs else 0,
                "total_volume": sum(prices)
            }
        
        current_metrics = calc_metrics(current_response.data)
        compare_metrics = calc_metrics(compare_response.data)
        
        # Calculate YoY changes
        def pct_change(current, previous):
            if previous == 0:
                return 0
            return ((current - previous) / previous) * 100
        
        report = {
            "success": True,
            "report_type": report_type,
            "building": building_name or "All Buildings",
            "current_period": {
                "year": year,
                **current_metrics
            },
            "comparison_period": {
                "year": compare_to_year,
                **compare_metrics
            },
            "year_over_year": {
                "sales_count_change": pct_change(current_metrics["count"], compare_metrics["count"]),
                "avg_price_change": pct_change(current_metrics["avg_price"], compare_metrics["avg_price"]),
                "avg_ppsf_change": pct_change(current_metrics["avg_ppsf"], compare_metrics["avg_ppsf"]),
                "volume_change": pct_change(current_metrics["total_volume"], compare_metrics["total_volume"])
            }
        }
        
        return report
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# TEAM TASK TOOL IMPLEMENTATIONS
# =============================================================================

def create_team_task(
    title: str,
    description: Optional[str] = None,
    status: str = "todo",
    priority: str = "medium",
    assigned_to_name: Optional[str] = None,
    due_date: Optional[str] = None
) -> dict:
    """Create a task in Team Workspace Kanban board."""
    try:
        supabase = get_supabase_client()
        
        task_data = {
            "title": title,
            "status": status,
            "priority": priority
        }
        
        if description:
            task_data["description"] = description
        
        if assigned_to_name:
            user_result = supabase.table("user_profiles").select("id, full_name").ilike("full_name", f"%{assigned_to_name}%").execute()
            if user_result.data:
                task_data["assigned_to"] = user_result.data[0]["id"]
        
        if due_date:
            task_data["due_date"] = due_date
        
        result = supabase.table("team_tasks").insert(task_data).execute()
        
        return {
            "success": True,
            "task_id": str(result.data[0]["id"]),
            "title": title,
            "status": status,
            "priority": priority,
            "message": f"Task '{title}' added to {status.replace('_', ' ').title()}" + 
                       (f", assigned to {assigned_to_name}" if assigned_to_name else "") +
                       (f", due {due_date}" if due_date else "")
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_team_tasks(
    status: Optional[str] = None,
    priority: Optional[str] = None,
    limit: int = 20
) -> dict:
    """Get tasks from Team Workspace Kanban board."""
    try:
        supabase = get_supabase_client()
        
        query = supabase.table("team_tasks").select(
            "id, title, description, status, priority, due_date, created_at"
        ).order("created_at", desc=True).limit(limit)
        
        if status:
            query = query.eq("status", status)
        if priority:
            query = query.eq("priority", priority)
        
        result = query.execute()
        
        tasks = []
        for task in result.data:
            tasks.append({
                "id": str(task["id"]),
                "title": task["title"],
                "description": task.get("description"),
                "status": task["status"],
                "priority": task["priority"],
                "due_date": task.get("due_date"),
                "created_at": task.get("created_at")
            })
        
        return {
            "success": True,
            "count": len(tasks),
            "tasks": tasks
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def update_task_status(
    task_id: Optional[str] = None,
    task_title: Optional[str] = None,
    new_status: Optional[str] = None,
    new_priority: Optional[str] = None
) -> dict:
    """Update a task status or priority."""
    if not task_id and not task_title:
        return {"success": False, "error": "Provide task_id or task_title"}
    
    try:
        supabase = get_supabase_client()
        
        if task_id:
            existing = supabase.table("team_tasks").select("id, title").eq("id", task_id).execute()
        else:
            existing = supabase.table("team_tasks").select("id, title").ilike("title", f"%{task_title}%").execute()
        
        if not existing.data:
            return {"success": False, "error": "Task not found"}
        
        task = existing.data[0]
        update_data = {}
        
        if new_status:
            update_data["status"] = new_status
        if new_priority:
            update_data["priority"] = new_priority
        
        if not update_data:
            return {"success": False, "error": "Nothing to update"}
        
        supabase.table("team_tasks").update(update_data).eq("id", task["id"]).execute()
        
        return {
            "success": True,
            "task_id": str(task["id"]),
            "title": task["title"],
            "message": f"Task '{task['title']}' updated"
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# TOOL DISPATCHER
# =============================================================================

def execute_tool(name: str, arguments: dict) -> Any:
    """Execute a tool by name with given arguments."""
    tool_map = {
        # Data Query Tools
        "query_active_listings": query_active_listings,
        "query_building_rankings": query_building_rankings,
        "query_market_cma": query_market_cma,
        "query_deal_of_week": query_deal_of_week,
        "search_airea_knowledge": search_airea_knowledge,
        "query_sales_history": query_sales_history,
        "get_building_list": get_building_list,
        "query_penthouse_listings": query_penthouse_listings,
        # Prospecting Tools
        "get_hot_leads": get_hot_leads,
        "query_stale_listings": query_stale_listings,
        # Content Tools
        "explain_deal_selection": explain_deal_selection,
        "generate_market_report": generate_market_report,
        # Team Task Tools
        "create_team_task": create_team_task,
        "get_team_tasks": get_team_tasks,
        "update_task_status": update_task_status,
    }
    
    if name not in tool_map:
        return {"error": f"Unknown tool: {name}"}
    
    try:
        return tool_map[name](**arguments)
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# MCP SERVER
# =============================================================================

async def main():
    """Run the MCP server."""
    server = Server("airea-data-tools")
    
    @server.list_tools()
    async def list_tools():
        return TOOLS
    
    @server.call_tool()
    async def call_tool(name: str, arguments: dict):
        logger.info(f"Tool called: {name} with args: {arguments}")
        result = execute_tool(name, arguments)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    
    logger.info("Starting AIREA Data Tools MCP Server...")
    logger.info(f"Tools available: {[t.name for t in TOOLS]}")
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, 
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
