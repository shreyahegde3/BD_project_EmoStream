# test_sender.py
import asyncio
import aiohttp
from rich.console import Console
from rich.progress import Progress
import time
import random

console = Console()

async def send_emoji(session, data):
    """Send a single emoji request"""
    url = 'http://localhost:5000/send_emoji'
    try:
        async with session.post(url, json=data) as response:
            return await response.json()
    except Exception as e:
        console.print(f"[red]Error sending emoji: {e}[/red]")
        return None

async def run_test_scenario(scenario_name, emoji_counts):
    """Run a specific test scenario"""
    console.print(f"\n[bold blue]Running {scenario_name}[/bold blue]")
    
    async with aiohttp.ClientSession() as session:
        with Progress() as progress:
            task = progress.add_task(f"[cyan]Sending emojis...", total=sum(count for _, count in emoji_counts))
            
            for emoji_type, count in emoji_counts:
                tasks = []
                for i in range(count):
                    data = {
                        'user_id': f'test_user_{i}',
                        'emoji_type': emoji_type,
                        'timestamp': int(time.time() * 1000)
                    }
                    tasks.append(send_emoji(session, data))
                    
                    if len(tasks) >= 100:  # Send in batches of 100
                        await asyncio.gather(*tasks)
                        progress.update(task, advance=len(tasks))
                        tasks = []
                        
                if tasks:  # Send remaining tasks
                    await asyncio.gather(*tasks)
                    progress.update(task, advance=len(tasks))
                
                await asyncio.sleep(0.1)  # Small delay between emoji types

async def main():
    test_scenarios = [
        ("Scenario 1: Single Emoji Burst (Should scale to 1)", [
            ('👍', 1200)  # More than 1000 of same emoji
        ]),
        
        ("Scenario 2: Multiple Emoji Types", [
            ('❤️', 800),   # Under 1000 - should show actual count
            ('😊', 1100),  # Over 1000 - should scale to 1
            ('🎉', 950)    # Under 1000 - should show actual count
        ]),
        
        ("Scenario 3: Mixed Volume Test", [
            ('👍', 500),
            ('❤️', 1200),
            ('😊', 300),
            ('🎉', 1500),
            ('👏', 700)
        ])
    ]
    
    for scenario_name, emoji_counts in test_scenarios:
        await run_test_scenario(scenario_name, emoji_counts)
        console.print(f"[green]Completed {scenario_name}[/green]")
        await asyncio.sleep(2)  # Wait for processing between scenarios

if __name__ == "__main__":
    console.print("[bold green]Starting EmoStream Test Suite[/bold green]")
    console.print("\n[yellow]Please ensure:[/yellow]")
    console.print("1. Flask app is running (python flaskapp.py)")
    console.print("2. Spark streaming job is running (python sparktask.py)")
    console.print("3. Kafka and Zookeeper are running")
    
    proceed = console.input("\nProceed with tests? (y/n): ")
    if proceed.lower() == 'y':
        asyncio.run(main())
    else:
        console.print("[red]Test cancelled[/red]")