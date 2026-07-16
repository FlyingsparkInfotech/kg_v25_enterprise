from rich.console import Console
from rich.panel import Panel
console = Console()
def info(msg): console.print(f"[cyan]→[/cyan] {msg}")
def ok(msg): console.print(f"[green]✅ {msg}[/green]")
def warn(msg): console.print(f"[yellow]⚠️ {msg}[/yellow]")
def banner(title): console.print(Panel.fit(title, border_style="cyan"))
