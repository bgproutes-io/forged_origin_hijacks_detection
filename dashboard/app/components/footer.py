import reflex as rx

def footer() -> rx.Component:
    return rx.box(
        rx.divider(),
        rx.hstack(
            rx.text("Funded by", size="2", weight="medium"),
            rx.link(
                rx.image(
                    src="/banner.png",   # 👈 local copy
                    alt="NLnet foundation logo",
                    width="140px",
                ),
                href="https://nlnet.nl",
                target="_blank",
            ),
            rx.link(
                rx.image(
                    src="/NGI0_tag.svg",  # 👈 local copy
                    alt="NGI Zero Logo",
                    width="140px",
                ),
                href="https://nlnet.nl/core",
                target="_blank",
            ),
            spacing="6",
            align="center",
            justify="center",
            wrap="wrap",
            width="100%",
            padding="1em",
        ),
        width="100%",
        margin_top="2em",
    )