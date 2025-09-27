import reflex as rx
from components.navbar import navbar
from components.footer import footer

@rx.page(route="/your_cases")
def your_cases() -> rx.Component:
    return rx.fragment(
        navbar(),
        rx.box(
            rx.heading("Your Cases", size="7", mb="4"),
            rx.text("This page will show your saved or custom cases."),
            width="100%",
            padding_x=["1rem","2rem","3rem","3rem"],
            padding_y="2rem",
            style={"maxWidth": "1600px", "margin": "0 auto"},
        ),
        footer(),
    )


