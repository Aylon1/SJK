import asyncio
from datetime import datetime
import pytz
from typing import Dict, Any, List, Optional
import logging

class PositionMarketReporter:
    """Handles formatting position and market updates for Telegram reporting."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @staticmethod
    def format_position_message(position: Dict[str, Any], market_data: Dict[str, Any]) -> str:
        """Format a position update message."""
        try:
            # Calculate P&L
            entry_price = position.get('entry_price', 0)
            current_price = market_data.get('price', 0)
            position_size = position.get('position_size', 0)
            side = position.get('side', 'UNKNOWN')
            
            # Calculate P&L based on position side
            if side.lower() == 'long':
                pnl = (current_price - entry_price) * position_size
            else:  # short
                pnl = (entry_price - current_price) * position_size
                
            pnl_pct = (pnl / (entry_price * position_size)) * 100 if entry_price * position_size != 0 else 0
            
            # Determine emoji based on P&L
            emoji = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
            
            return (
                f"{emoji} Position Update - {position.get('symbol')}\n\n"
                f"Side: {side}\n"
                f"Size: {position_size:.8f}\n"
                f"Value: ${(current_price * position_size):.2f}\n\n"
                f"Entry: ${entry_price:.2f}\n"
                f"Current: ${current_price:.2f}\n"
                f"PnL: ${pnl:.2f} ({pnl_pct:.2f}%)\n\n"
                f"Risk Management:\n"
                f"Stop Loss: ${position.get('stop_loss', 0):.2f}\n"
                f"Take Profit: ${position.get('take_profit', 0):.2f}\n"
                f"Liquidation: ${position.get('liquidation_price', 0):.2f}\n\n"
                f"Market Status:\n"
                f"24h Change: {market_data.get('change_24h', 0):.2f}%\n"
                f"Volume: ${market_data.get('volume_24h', 0):,.2f}\n"
                f"Volatility: {market_data.get('volatility', 0):.2f}%\n\n"
                f"Updated: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except Exception as e:
            return f"Error formatting position message: {str(e)}"

    @staticmethod
    def format_market_message(symbol: str, market_data: Dict[str, Any], order_book: Dict[str, Any]) -> str:
        """Format a market snapshot message."""
        try:
            # Determine trend emoji
            change_24h = market_data.get('change_24h', 0)
            trend = "📈" if change_24h > 0 else "📉" if change_24h < 0 else "➡️"
            
            current_price = market_data.get('price', 0)
            spread = order_book.get('spread', 0)
            spread_pct = (spread / current_price * 100) if current_price > 0 else 0
            
            return (
                f"{trend} Market Update - {symbol}\n\n"
                f"Price Action:\n"
                f"Current: ${current_price:.2f}\n"
                f"24h Change: {change_24h:.2f}%\n"
                f"24h High: ${market_data.get('high_24h', 0):.2f}\n"
                f"24h Low: ${market_data.get('low_24h', 0):.2f}\n\n"
                f"Volume & Liquidity:\n"
                f"24h Volume: ${market_data.get('volume_24h', 0):,.2f}\n"
                f"Bid Depth: ${order_book.get('bid_depth', 0):,.2f}\n"
                f"Ask Depth: ${order_book.get('ask_depth', 0):,.2f}\n"
                f"Spread: ${spread:.2f} ({spread_pct:.3f}%)\n\n"
                f"Market Metrics:\n"
                f"Volatility: {market_data.get('volatility', 0):.2f}%\n"
                f"Liquidity Score: {market_data.get('liquidity_score', 0):.1f}/100\n"
                f"Market Impact: {order_book.get('price_impact_buy', 0):.3f}%\n\n"
                f"Updated: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except Exception as e:
            return f"Error formatting market message: {str(e)}"

    @staticmethod
    def format_portfolio_message(positions: List[Dict[str, Any]], portfolio_value: float) -> str:
        """Format a portfolio summary message."""
        try:
            # Calculate portfolio metrics
            total_pnl = sum(pos.get('pnl', 0) for pos in positions)
            total_exposure = sum(pos.get('position_size', 0) * pos.get('current_price', 0) 
                               for pos in positions)
            exposure_ratio = (total_exposure / portfolio_value * 100) if portfolio_value > 0 else 0
            
            # Determine overall status emoji
            status_emoji = "🟢" if total_pnl > 0 else "🔴" if total_pnl < 0 else "⚪"
            
            # Format positions by side
            long_positions = [p for p in positions if p.get('side', '').lower() == 'long']
            short_positions = [p for p in positions if p.get('side', '').lower() == 'short']
            
            return (
                f"{status_emoji} Portfolio Summary\n\n"
                f"Portfolio Status:\n"
                f"Total Value: ${portfolio_value:,.2f}\n"
                f"Total PnL: ${total_pnl:,.2f}\n"
                f"Total Exposure: ${total_exposure:,.2f}\n"
                f"Exposure Ratio: {exposure_ratio:.1f}%\n\n"
                f"Long Positions ({len(long_positions)}):\n"
                + "\n".join([
                    f"• {pos.get('symbol')}: "
                    f"{pos.get('position_size', 0):.8f} "
                    f"(${pos.get('pnl', 0):.2f})"
                    for pos in long_positions
                ]) + "\n\n"
                f"Short Positions ({len(short_positions)}):\n"
                + "\n".join([
                    f"• {pos.get('symbol')}: "
                    f"{pos.get('position_size', 0):.8f} "
                    f"(${pos.get('pnl', 0):.2f})"
                    for pos in short_positions
                ]) + "\n\n"
                f"Updated: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except Exception as e:
            return f"Error formatting portfolio message: {str(e)}"

    @staticmethod
    def format_risk_message(risk_metrics: Dict[str, Any]) -> str:
        """Format a risk metrics message."""
        try:
            risk_level = risk_metrics.get('risk_level', 'unknown')
            emoji = {
                'very_low': '🟢',
                'low': '🟢',
                'medium': '🟡',
                'high': '🔴',
                'very_high': '🔴',
                'unknown': '⚪'
            }.get(risk_level, '⚪')
            
            # Get margin and leverage metrics
            margin_used = risk_metrics.get('margin_utilization', 0)
            leverage = risk_metrics.get('leverage_ratio', 1)
            
            # Get risk alerts
            alerts = []
            if margin_used > 80:
                alerts.append("⚠️ High margin usage")
            if leverage > 3:
                alerts.append("⚠️ High leverage")
            if risk_metrics.get('concentration_score', 0) > 0.5:
                alerts.append("⚠️ High concentration")
            
            return (
                f"{emoji} Risk Metrics Update\n\n"
                f"Risk Status:\n"
                f"Risk Level: {risk_level.upper()}\n"
                f"Largest Position: {risk_metrics.get('largest_position_pct', 0):.1f}%\n"
                f"Leverage: {leverage:.2f}x\n"
                f"Margin Usage: {margin_used:.1f}%\n\n"
                f"Portfolio Risk:\n"
                f"Concentration Score: {risk_metrics.get('concentration_score', 0):.2f}\n"
                f"Daily VaR: ${risk_metrics.get('var_95', 0):.2f}\n"
                f"Max Drawdown: {risk_metrics.get('max_drawdown', 0):.1f}%\n"
                + (f"\nRisk Alerts:\n{chr(10).join(alerts)}" if alerts else "")
                + f"\n\nUpdated: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except Exception as e:
            return f"Error formatting risk message: {str(e)}"