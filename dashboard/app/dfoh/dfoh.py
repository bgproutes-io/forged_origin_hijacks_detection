import reflex as rx

# Import pages
from pages.index import index
from pages.your_cases import your_cases
from pages.documentation import documentation
from pages.detail import detail_page

app = rx.App()

# Register pages
app.add_page(index, title="New BGP Links")
app.add_page(your_cases, title="Your Cases")
app.add_page(documentation, title="Documentation")
app.add_page(detail_page, title="Case Details")

