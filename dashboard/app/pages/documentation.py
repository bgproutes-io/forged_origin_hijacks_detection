import reflex as rx
from pathlib import Path
from components.navbar import navbar
from components.footer import footer

def documentation_page() -> rx.Component:
    md_file = Path("assets/documentation.md")
    md_content = md_file.read_text(encoding="utf-8") if md_file.exists() else "# Documentation\nFile not found."
    return rx.markdown(md_content)

@rx.page(route="/documentation")
def documentation() -> rx.Component:
    return rx.fragment(
        navbar(),
        rx.box(
            rx.heading("Documentation", size="7", mb="4"),
            documentation_page(),
            width="100%",
            padding_x=["1rem","2rem","3rem","3rem"],
            padding_y="2rem",
            style={"maxWidth": "1400px", "margin": "0 auto"},
        ),
        footer(),
    )

