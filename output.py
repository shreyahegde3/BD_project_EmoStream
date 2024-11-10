# output_monitor.py
from confluent_kafka import Consumer
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
import time

# Initialize Rich console for pretty printing
console = Console()

# Kafka Consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'emoji_monitor_group',
    'auto.offset.reset': 'latest'
}

def format_timestamp(ts):
    return datetime.fromtimestamp(ts/1000).strftime('%H:%M:%S')

def monitor_processed_emojis():
    consumer = Consumer(consumer_config)
    consumer.subscribe(['processed_emojis'])
    
    current_window_data = {}
    
    try:
        with Live(refresh_per_second=2) as live:
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    console.print(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    data = json.loads(msg.value())
                    window_start = format_timestamp(data['window']['start'])
                    window_end = format_timestamp(data['window']['end'])
                    emoji_type = data['emoji_type']
                    scaled_count = data['scaled_count']
                    
                    window_key = f"{window_start}-{window_end}"
                    if window_key not in current_window_data:
                        current_window_data[window_key] = {}
                    current_window_data[window_key][emoji_type] = scaled_count
                    
                    # Create table for display
                    table = Table(title="Real-time Emoji Analytics")
                    table.add_column("Time Window")
                    table.add_column("Emoji Type")
                    table.add_column("Count")
                    
                    # Only keep last 5 windows
                    windows = sorted(current_window_data.keys())[-5:]
                    for window in windows:
                        for emoji, count in current_window_data[window].items():
                            table.add_row(window, emoji, str(count))
                    
                    live.update(table)
                    
                except json.JSONDecodeError as e:
                    console.print(f"Error decoding message: {e}")
                    
    except KeyboardInterrupt:
        consumer.close()