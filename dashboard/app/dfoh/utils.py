import reflex as rx

max_width = "1400px"  # same value as your table/content

def navbar() -> rx.Component:
    return rx.box(
        rx.flex(
            # --- Inner constrained content ---
            rx.hstack(
                # Left side: title instead of images
                rx.link(
                    rx.text(
                        "DFOH",
                        size="6",
                        weight="bold",
                        color=rx.color_mode_cond(
                            light="black",
                            dark="white",
                        ),
                        style={
                            "letterSpacing": "0.05em",
                            "fontFamily": "sans-serif",
                        },
                    ),
                    href="/",
                ),
                rx.spacer(),
                # Right side: navigation links
                rx.hstack(
                    rx.link(rx.text("New Links", size="4", weight="medium"), href="/"),
                    rx.link(rx.text("Your Cases", size="4", weight="medium"), href="/your_cases"),
                    rx.link(rx.text("Documentation", size="4", weight="medium"), href="/documentation"),
                    spacing="6",
                    align="center",
                ),
                align="center",
                justify="between",
                width="100%",
            ),
            width="100%",
            max_width=max_width,     # ✅ constrain elements
            margin="0 auto",         # ✅ center inside full-width bar
            padding_x="2em",
            padding_y="1em",
        ),
        width="100%",                # ✅ full screen background
        bg=rx.color_mode_cond(
            light="#DDDDDD",
            dark="#222222",
        ),
        position="fixed",            # ✅ sticks to top
        top="0",
        z_index="100",
        box_shadow="sm",
    )