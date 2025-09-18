

import os
import json
from google.cloud import bigquery
from pydantic import BaseModel, Field
from typing import Dict, Any, List


from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.tools import FunctionTool, ToolContext, agent_tool


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or "sad-2025-apprentice-playground"
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    print("BigQuery client initialized successfully.")
except Exception as e:
    print(f"Could not initialize BigQuery client. Error: {e}")
    bq_client = None

def execute_bq_query(query: str):
    if not bq_client:
        raise Exception("BigQuery client is not available.")
    try:
        print(f"--- EXECUTING BQ QUERY ---\n{query}\n--------------------------")
        query_job = bq_client.query(query)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        print(f"BigQuery query failed: {e}")
        return {"error": str(e)}

class SqlQueries(BaseModel):
    ga_query: str = Field(description="BigQuery SQL for mock_ga_sessions.")
    gsc_query: str = Field(description="BigQuery SQL for mock_gsc_performance.")
    yt_query: str = Field(description="BigQuery SQL for mock_youtube_analytics.")
   


import requests 

def update_website_metadata(
    title: str, 
    description: str, 
    page_h1: str, 
    page_paragraph: str
) -> Dict[str, str]:
    """
    Calls the website's API to update the SEO metadata AND the main visible content.

    Args:
        title (str): The new SEO title.
        description (str): The new meta description.
        page_h1 (str): The new main heading (h1) for the page.
        page_paragraph (str): The new introductory paragraph for the page.

    Returns:
        Dict[str, str]: The status of the API call.
    """
    service_url = "https://apollo-mock-web-223582690079.us-central1.run.app/update-metadata"
    
   
    payload = {
        "title": title, 
        "description": description,
        "page_h1": page_h1,
        "page_paragraph": page_paragraph
    }
    headers = {"Content-Type": "application/json"}

    try:
        print(f"--- CALLING METADATA API ---\nURL: {service_url}\nPayload: {json.dumps(payload, indent=2)}\n--------------------------")
        response = requests.post(service_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API call failed: {e}")
        return {"status": "error", "message": f"API call failed: {e}"}


def execute_all_queries(queries: List[str]) -> Dict[str, Any]:
    """
    Executes a list of three BigQuery SQL queries in order (GA, GSC, YouTube).

    Args:
        queries (List[str]): A list containing exactly three SQL query strings.
                             The order MUST be: [ga_query, gsc_query, yt_query].

    Returns:
        Dict[str, Any]: A dictionary containing the results of each query.
    """
    results = {}
    if not isinstance(queries, list) or len(queries) != 3:
        return {"error": "Tool error: Input must be a list of exactly three query strings."}

    try:
       
        ga_query, gsc_query, yt_query = queries
        results["ga_results"] = execute_bq_query(ga_query)
        results["gsc_results"] = execute_bq_query(gsc_query)
        results["yt_results"] = execute_bq_query(yt_query)
        return results
    except Exception as e:
        return {"error": f"Tool failed during query execution: {e}"}





query_generator_agent = LlmAgent(
    name="QueryGeneratorAgent",
    model="gemini-2.0-flash",
    instruction="""You are an expert Google BigQuery data analyst. Your goal is to
    write three robust and efficient SQL queries based on the user's request for
    a specific hospital department (e.g., 'Orthopedics').

    Your output MUST be a single, valid JSON object that conforms to the SqlQueries schema.

    Follow these critical rules for query generation:
    1.  **Use Full Table Paths**: Use the full paths: `sad-2025-apprentice-playground.hospital_demo.ga_sessions`,
        `sad-2025-apprentice-playground.hospital_demo.gsc_performance`, and
        `sad-2025-apprentice-playground.hospital_demo.youtube_analytics`.
    2.  **Case-Insensitive Search**: ALL `LIKE` comparisons MUST be case-insensitive.
        Use the `LOWER()` function on the column (e.g., `LOWER(query) LIKE '%orthopedic%'`).
    3.  **Broad Topic Matching**: For the 'Orthopedics' topic, your WHERE clause
        must check for multiple related keywords: 'orthopedic', 'knee', 'hip',
        'shoulder', and 'rotator'.
    4.  **Handle JSON Columns**: The `hits` and `totals` columns are JSON STRINGS.
        - To query the `hits` array, you MUST use the pattern:
          `... FROM \`...ga_sessions\`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json`
          and filter using `JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')`.
        - To access fields in the `totals` string (like `pageviews`), you MUST parse
          it using `JSON_EXTRACT_SCALAR(totals, '$.fieldname')` and `CAST` the
          result to a number, for example: `CAST(... AS INT64)`.
    5.  **AVOID DUPLICATION WITH SUBQUERIES**: When a query on `ga_sessions`
        requires both aggregating `totals` fields (like pageviews) AND filtering on `hits`
        fields (like page_path), you MUST use a subquery to prevent incorrect results.
        First, find the `session_id`s in a subquery, then aggregate in the outer query.
        Follow this template:
        ```sql
        SELECT
          COUNT(DISTINCT session_id) AS total_sessions,
          SUM(CAST(JSON_EXTRACT_SCALAR(totals, '$.pageviews') AS INT64)) AS total_pageviews
        FROM
          `sad-2025-apprentice-playground.hospital_demo.ga_sessions`
        WHERE session_id IN (
          SELECT DISTINCT session_id
          FROM `sad-2025-apprentice-playground.hospital_demo.ga_sessions`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json
          WHERE LOWER(JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')) LIKE '%keyword%'
        );
        ```

    ---
    -- Here are the complete table schemas you must use:
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.ga_sessions` (
      partition_date DATE, session_id STRING, user_id STRING,
      device_category STRING, browser STRING, operating_system STRING,
      geo STRUCT<country STRING, region STRING, city STRING>,
      traffic_source STRUCT<source STRING, medium STRING, campaign STRING>,
      totals STRUCT<pageviews INT64, time_on_site_seconds INT64, bounces INT64>,
      hits ARRAY<STRUCT<hit_number INT64, type STRING, page STRUCT<page_path STRING, page_title STRING>>>
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.gsc_performance` (
      partition_date DATE, query STRING, page_url STRING, country STRING,
      device STRING, clicks INT64, impressions INT64
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.youtube_analytics` (
      partition_date DATE, external_video_id STRING, video_title STRING,
      country_code STRING, views INT64, watch_time_msec INT64
    );
    ---
    """,
    
    output_key="generated_queries"
)

data_fetcher_agent = LlmAgent(
    name="DataFetcherAgent", model="gemini-2.0-flash",
    instruction="Your job is to execute the BigQuery queries from the session state key {generated_queries} by calling the `execute_all_queries` tool. pass the generated queries to the tool.",
    tools=[execute_all_queries], output_key="fetched_data"
)


recommendation_generator_agent = LlmAgent(
    name="RecommendationGeneratorAgent", model="gemini-2.0-flash",
    instruction="""You are a marketing analyst. First, analyze the raw {fetched_data}
    from the session state and provide a concise, bulleted summary.
    Second, based on that summary, formulate three distinct recommendations 
    with 'Action', 'Pros', and 'Risks/Cons'. output all recomendations in proper format and Conclude by asking the user what
    they would like to do next (e.g., 'Which recommendation would you like to
    explore further, or do you have any questions about these suggestions?').""",
    output_key="recommendations_text" 
)

performance_analysis_pipeline = SequentialAgent(
    name="PerformanceAnalysisPipeline",
    description="A multi-step tool that performs a full performance analysis: it queries databases for website, search, and YouTube data, then generates a summary and three high-level recommendations (A, B, C).",
    sub_agents=[
        query_generator_agent,
        data_fetcher_agent,
        recommendation_generator_agent,
    ],
)


faq_agent = LlmAgent(
    name="FAQAgent",
    model="gemini-2.0-flash",
    description="Answers user's clarifying questions or concerns about the marketing recommendations that have just been presented.",
    instruction="""You are a helpful strategy assistant. The user has just been
    shown a set of marketing recommendations and is asking a clarifying question.
    The original recommendations are available in the session state under the key
    recommendations_text . Use this context to provide a detailed and helpful
    explanation. For example, if they ask about 'content saturation', explain what
    it means in the context of SEO and why it was listed as a risk.
    
    you can use execute all queries tool in case to extract some data to support your insights , here is bigquery schema 
    follow these critical rules for query generation:
    1.  **Use Full Table Paths**: Use the full paths: `sad-2025-apprentice-playground.hospital_demo.ga_sessions`,
        `sad-2025-apprentice-playground.hospital_demo.gsc_performance`, and
        `sad-2025-apprentice-playground.hospital_demo.youtube_analytics`.
    2.  **Case-Insensitive Search**: ALL `LIKE` comparisons MUST be case-insensitive.
        Use the `LOWER()` function on the column (e.g., `LOWER(query) LIKE '%orthopedic%'`).
    3.  **Broad Topic Matching**: For the 'Orthopedics' topic, your WHERE clause
        must check for multiple related keywords: 'orthopedic', 'knee', 'hip',
        'shoulder', and 'rotator'.
    4.  **Handle JSON Columns**: The `hits` and `totals` columns are JSON STRINGS.
        - To query the `hits` array, you MUST use the pattern:
          `... FROM \`...ga_sessions\`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json`
          and filter using `JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')`.
        - To access fields in the `totals` string (like `pageviews`), you MUST parse
          it using `JSON_EXTRACT_SCALAR(totals, '$.fieldname')` and `CAST` the
          result to a number, for example: `CAST(... AS INT64)`.
    5.  **AVOID DUPLICATION WITH SUBQUERIES**: When a query on `ga_sessions`
        requires both aggregating `totals` fields (like pageviews) AND filtering on `hits`
        fields (like page_path), you MUST use a subquery to prevent incorrect results.
        First, find the `session_id`s in a subquery, then aggregate in the outer query.
        Follow this template:
        ```sql
        SELECT
          COUNT(DISTINCT session_id) AS total_sessions,
          SUM(CAST(JSON_EXTRACT_SCALAR(totals, '$.pageviews') AS INT64)) AS total_pageviews
        FROM
          `sad-2025-apprentice-playground.hospital_demo.ga_sessions`
        WHERE session_id IN (
          SELECT DISTINCT session_id
          FROM `sad-2025-apprentice-playground.hospital_demo.ga_sessions`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json
          WHERE LOWER(JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')) LIKE '%keyword%'
        );
        ```

    ---
    -- Here are the complete table schemas you must use:
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.ga_sessions` (
      partition_date DATE, session_id STRING, user_id STRING,
      device_category STRING, browser STRING, operating_system STRING,
      geo STRUCT<country STRING, region STRING, city STRING>,
      traffic_source STRUCT<source STRING, medium STRING, campaign STRING>,
      totals STRUCT<pageviews INT64, time_on_site_seconds INT64, bounces INT64>,
      hits ARRAY<STRUCT<hit_number INT64, type STRING, page STRUCT<page_path STRING, page_title STRING>>>
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.gsc_performance` (
      partition_date DATE, query STRING, page_url STRING, country STRING,
      device STRING, clicks INT64, impressions INT64
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.youtube_analytics` (
      partition_date DATE, external_video_id STRING, video_title STRING,
      country_code STRING, views INT64, watch_time_msec INT64
    );
    """,
    tools=[execute_all_queries]
)


deep_dive_agent = LlmAgent(
    name="DeepDiveAgent",
    model="gemini-2.0-flash",
    description="Provides a detailed breakdown and action plan for a specific recommendation (A, B, or C) that the user has chosen to proceed with.",
    instruction="""You are a detailed marketing analyst. The user has made a clear
    choice to proceed with a specific recommendation. Their choice is in the
    latest user message. Provide a detailed analysis for that recommendation,
    including specific topics, benefits, risks, and mitigations as outlined in
    the system prompt.
    
    you can use execute all queries tool in case to extract some data to support your insights , here is bigquery schema 
    follow these critical rules for query generation:
    1.  **Use Full Table Paths**: Use the full paths: `sad-2025-apprentice-playground.hospital_demo.ga_sessions`,
        `sad-2025-apprentice-playground.hospital_demo.gsc_performance`, and
        `sad-2025-apprentice-playground.hospital_demo.youtube_analytics`.
    2.  **Case-Insensitive Search**: ALL `LIKE` comparisons MUST be case-insensitive.
        Use the `LOWER()` function on the column (e.g., `LOWER(query) LIKE '%orthopedic%'`).
    3.  **Broad Topic Matching**: For the 'Orthopedics' topic, your WHERE clause
        must check for multiple related keywords: 'orthopedic', 'knee', 'hip',
        'shoulder', and 'rotator'.
    4.  **Handle JSON Columns**: The `hits` and `totals` columns are JSON STRINGS.
        - To query the `hits` array, you MUST use the pattern:
          `... FROM \`...ga_sessions\`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json`
          and filter using `JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')`.
        - To access fields in the `totals` string (like `pageviews`), you MUST parse
          it using `JSON_EXTRACT_SCALAR(totals, '$.fieldname')` and `CAST` the
          result to a number, for example: `CAST(... AS INT64)`.
    5.  **AVOID DUPLICATION WITH SUBQUERIES**: When a query on `ga_sessions`
        requires both aggregating `totals` fields (like pageviews) AND filtering on `hits`
        fields (like page_path), you MUST use a subquery to prevent incorrect results.
        First, find the `session_id`s in a subquery, then aggregate in the outer query.
        Follow this template:
        ```sql
        SELECT
          COUNT(DISTINCT session_id) AS total_sessions,
          SUM(CAST(JSON_EXTRACT_SCALAR(totals, '$.pageviews') AS INT64)) AS total_pageviews
        FROM
          `sad-2025-apprentice-playground.hospital_demo.ga_sessions`
        WHERE session_id IN (
          SELECT DISTINCT session_id
          FROM `sad-2025-apprentice-playground.hospital_demo.ga_sessions`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json
          WHERE LOWER(JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')) LIKE '%keyword%'
        );
        ```

    ---
    -- Here are the complete table schemas you must use:
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.ga_sessions` (
      partition_date DATE, session_id STRING, user_id STRING,
      device_category STRING, browser STRING, operating_system STRING,
      geo STRUCT<country STRING, region STRING, city STRING>,
      traffic_source STRUCT<source STRING, medium STRING, campaign STRING>,
      totals STRUCT<pageviews INT64, time_on_site_seconds INT64, bounces INT64>,
      hits ARRAY<STRUCT<hit_number INT64, type STRING, page STRUCT<page_path STRING, page_title STRING>>>
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.gsc_performance` (
      partition_date DATE, query STRING, page_url STRING, country STRING,
      device STRING, clicks INT64, impressions INT64
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.youtube_analytics` (
      partition_date DATE, external_video_id STRING, video_title STRING,
      country_code STRING, views INT64, watch_time_msec INT64
    );


    ---
    **Special Instruction for SEO/Metadata Recommendations:**
    If the recommendation involves changing website metadata, you MUST **propose a
    specific new title and description, h1 heading, paragraph** as part of your plan.
    **Do NOT call any tools to update the website.** Simply present the suggested
    metadata to the user and let them decide.
    ---

    """,
    tools=[execute_all_queries, update_website_metadata]
)
metadata_updater_agent = LlmAgent(
    name="MetadataUpdaterAgent",
    model="gemini-1.5-flash",
    description="Updates the website's live title, description, and visible hero content.",
    instruction="""You are an execution agent. The user has confirmed they want
    to update the website's content.
    1. Extract the title, description, h1 heading, and paragraph from the user's prompt or context.
    2. Call the `update_website_metadata` tool with all four pieces of content to push the changes live.
    3. Confirm to the user that the website has been updated.""",
    tools=[update_website_metadata],
)

data_query_agent = LlmAgent(
    name="DataQueryAgent",
    model="gemini-2.0-flash",
    description="Answers specific, data-driven questions by writing and executing a single BigQuery query.",
    instruction="""You are a data analyst. Your job is to answer the user's specific
    question by writing and executing a single, efficient BigQuery SQL query
    using the `execute_bq_query` tool. Use the full table paths provided in the
    schema. After getting the results, present them to the user in a clear,
    human-readable format. dont show query to user unles user asks for it, just show results

    for eg : list webpages with least traffic
    query : "SELECT JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path') AS page_path, COUNT(*) AS pageviews FROM `sad-2025-apprentice-playground.hospital_demo.ga_sessions`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json WHERE JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path') IS NOT NULL GROUP BY 1 ORDER BY 2 ASC LIMIT 10"
    
       you can use execute all queries tool in case to extract some data to support your insights , here is bigquery schema 
    follow these critical rules for query generation:
    1.  **Use Full Table Paths**: Use the full paths: `sad-2025-apprentice-playground.hospital_demo.ga_sessions`,
        `sad-2025-apprentice-playground.hospital_demo.gsc_performance`, and
        `sad-2025-apprentice-playground.hospital_demo.youtube_analytics`.
    2.  **Case-Insensitive Search**: ALL `LIKE` comparisons MUST be case-insensitive.
        Use the `LOWER()` function on the column (e.g., `LOWER(query) LIKE '%orthopedic%'`).
    3.  **Broad Topic Matching**: For the 'Orthopedics' topic, your WHERE clause
        must check for multiple related keywords: 'orthopedic', 'knee', 'hip',
        'shoulder', and 'rotator'.
    4.  **Handle JSON Columns**: The `hits` and `totals` columns are JSON STRINGS.
        - To query the `hits` array, you MUST use the pattern:
          `... FROM \`...ga_sessions\`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json`
          and filter using `JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')`.
        - To access fields in the `totals` string (like `pageviews`), you MUST parse
          it using `JSON_EXTRACT_SCALAR(totals, '$.fieldname')` and `CAST` the
          result to a number, for example: `CAST(... AS INT64)`.
    5.  **AVOID DUPLICATION WITH SUBQUERIES**: When a query on `ga_sessions`
        requires both aggregating `totals` fields (like pageviews) AND filtering on `hits`
        fields (like page_path), you MUST use a subquery to prevent incorrect results.
        First, find the `session_id`s in a subquery, then aggregate in the outer query.
        Follow this template:
        ```sql
        SELECT
          COUNT(DISTINCT session_id) AS total_sessions,
          SUM(CAST(JSON_EXTRACT_SCALAR(totals, '$.pageviews') AS INT64)) AS total_pageviews
        FROM
          `sad-2025-apprentice-playground.hospital_demo.ga_sessions`
        WHERE session_id IN (
          SELECT DISTINCT session_id
          FROM `sad-2025-apprentice-playground.hospital_demo.ga_sessions`, UNNEST(JSON_QUERY_ARRAY(hits)) AS hit_json
          WHERE LOWER(JSON_EXTRACT_SCALAR(hit_json, '$.page.page_path')) LIKE '%keyword%'
        );
        ```

    ---
    -- Here are the complete table schemas you must use:
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.ga_sessions` (
      partition_date DATE, session_id STRING, user_id STRING,
      device_category STRING, browser STRING, operating_system STRING,
      geo STRUCT<country STRING, region STRING, city STRING>,
      traffic_source STRUCT<source STRING, medium STRING, campaign STRING>,
      totals STRUCT<pageviews INT64, time_on_site_seconds INT64, bounces INT64>,
      hits ARRAY<STRUCT<hit_number INT64, type STRING, page STRUCT<page_path STRING, page_title STRING>>>
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.gsc_performance` (
      partition_date DATE, query STRING, page_url STRING, country STRING,
      device STRING, clicks INT64, impressions INT64
    );
    CREATE TABLE `sad-2025-apprentice-playground.hospital_demo.youtube_analytics` (
      partition_date DATE, external_video_id STRING, video_title STRING,
      country_code STRING, views INT64, watch_time_msec INT64
    );

    
    """,
    tools=[execute_bq_query]
)



root_agent = LlmAgent(
    name="RootOrchestratorAgent",
    model="gemini-2.0-flash",
    description="The main orchestrator for the digital marketing assistant.",
    instruction="""You are the lead marketing strategist orchestrating a team of specialist agents. Your primary goal is to manage the conversation flow based on the user's request.

    1.  **Analyze the User's Intent:**
        -   If the request is for a **high-level analysis, report, or strategy** (e.g., 'analyze orthopedics performance', 'suggest content strategies'), you MUST use the `PerformanceAnalysisPipeline` tool.
        -   If the request is a **specific data question** (e.g., 'list webpages with least traffic', 'what are our top keywords?'), you MUST delegate to the `DataQueryAgent`.

    2.  **Handle Follow-up Requests** (after the main pipeline has run):
        -   If the user asks a **clarifying question** about the report, delegate to the `FAQAgent`.
        -   If the user **chooses a recommendation** (e.g., 'Let's do A', 'explore the SEO option'), delegate to the `DeepDiveAgent`.
        -   If the user gives an **explicit command to update the website** (e.g., 'go ahead and update the site', 'use that title and description'), delegate to the `MetadataUpdaterAgent`.
    """,
    tools=[agent_tool.AgentTool(agent=performance_analysis_pipeline)],
    sub_agents=[
        data_query_agent, 
        faq_agent,
        deep_dive_agent,
        metadata_updater_agent 
    ],
)