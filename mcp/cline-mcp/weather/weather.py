from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer

# Initialize the MCP server (mcp 2.x: FastMCP 已更名为 MCPServer)
mcp = MCPServer("weather", log_level="ERROR")

#Constants - 请求这个URL来拿美国的天气信息
NWS_API_BASE = "https://api.weather.gov"
USER_AGENT = "weather-app/1.0"

async def make_nws_request(url: str) -> dict[str, Any] | None:
    """Make a request to NWS API with proper error handling."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/geo+json"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

def format_alert(feature: dict) -> str:
    """Format an alert feature into  a readable string."""
    props = feature["properties"]
    return f"""
Event: {props.get('event', 'Unknown')}
Area: {props.get('areaDesc', 'Unknown')}
Severity: {props.get('severity', 'Unknown')}
Description: {props.get('description', 'No description available')}
Instructions: {props.get('instruction', 'No specific instructions provided')}
"""

@mcp.tool()
async def get_alerts(state: str) -> str:
    """Get weather alerts for a US state.

    Args:
        state: Two-letter US state code (e.g. CA, NY)
    """
    url = f"{NWS_API_BASE}/alerts/active/area/{state}"
    data = await make_nws_request(url)

    if not data or "features" not in data:
        return "Unable to fetch alert or no alerts found."

    if not data["features"]:
        return "No active alerts for this state."

    alerts = [format_alert(feature) for feature in data["features"]]
    return "\n---\n".join(alerts)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float) -> str:
    """Get weather forecast for a location.

    Args:
        latitude: Latitude or the location
        longitude: Longitude of the location
    """
    # First get the forecast grid endpoint for the given coordinates
    points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
    points_data = await make_nws_request(points_url)

    if not points_data:
        return "Unable to fetch forecast data for this location."
    
    # Get the forecast URL from the points response
    forecast_url = (points_data.get("properties") or {}).get("forecast")

    if not forecast_url:
        return "Unable to resolve the forecast URL for this location."

    forecast_data = await make_nws_request(forecast_url)

    if not forecast_data:
        return "Unable to fetch detailed forecast."
    
    # Format the periods into a readable forcast
    periods = (forecast_data.get("properties") or {}).get("periods")

    if not periods:
        return "No forecast periods returned for this location."

    forecasts = []
    for period in periods[:5]: # Only show next 5 periods
        forecast = f"""
{period['name']}:
Temperature: {period['temperature']} {period['temperatureUnit']}
Wind: {period['windSpeed']} {period['windDirection']}
Forecast: {period['detailedForecast']}
"""
        forecasts.append(forecast)

    return "\n---\n".join(forecasts)


if __name__ == "__main__":
    # 截获与客户端（Cline）的 MCP 交互并写入 weather_mcp.log，必须在 mcp.run() 之前安装
    import mcp_traffic_log

    mcp_traffic_log.install()

    # Initialize and run the server
    mcp.run(transport='stdio')