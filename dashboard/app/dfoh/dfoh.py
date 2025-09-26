import reflex as rx
import requests



def case_is_correct(case_ :dict[str, str | int | list[str]]):
    if "date" not in case_:
        return False, None
    
    if "as1" not in case_:
        return False, None
    
    if "as2" not in case_:
        return False, None
    
    if "presumed_attacker" not in case_:
        return False, None
    
    if "presumed_victims" not in case_:
        return False, None
    
    if "inference_result" not in case_:
        return False, None
    
    if "confidence_level" not in case_:
        return False, None
    
    if "nb_aspaths_observed" not in case_:
        return False, None
    
    if "is_reccurent" not in case_:
        return False, None
    
    return True, (case_["date"].replace("T", " "), str(case_["as1"]), str(case_["as2"]), [str(x) for x in case_["presumed_attacker"]], [str(x) for x in case_["presumed_victims"]], case_["inference_result"], case_["confidence_level"], case_["nb_aspaths_observed"], case_["is_reccurent"])



# ---- STATE ----
class NewLinksState(rx.State):
    links: list[tuple[str, str, str, list[str], list[str], str, int, int, bool]] = []
    loading: bool = False

    @rx.event(background=True)
    async def load_links(self):
        """Fetch links from API and populate the state."""
        async with self:
            self.loading = True
        yield

        try:
            resp = requests.get("https://dfoh-api.bgproutes.io/new_links", timeout=10)
            if resp.status_code == 200:
                data = resp.json()["results"]
            else:
                data = list()
        except Exception as e:
            data = list()

        async with self:
            self.links = list()
            for case_ in data:
                ok, val = case_is_correct(case_)

                if ok:
                    self.links.append(val)

            self.links = sorted(self.links, key=lambda x:x[0])
            self.loading = False
        yield


# ---- UTILS ----
def safe_join(value):
    """Ensure value is always a string list."""
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value)
    if value is None:
        return ""
    return str(value)


# ---- TABLE ----
def new_links_table() -> rx.Component:
    return rx.card(
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Date"),
                    rx.table.column_header_cell("AS1"),
                    rx.table.column_header_cell("AS2"),
                    rx.table.column_header_cell("Presumed Attacker"),
                    rx.table.column_header_cell("Presumed Victims"),
                    rx.table.column_header_cell("Inference"),
                    rx.table.column_header_cell("Confidence"),
                    rx.table.column_header_cell("Observed Paths"),
                    rx.table.column_header_cell("Recurrent"),
                )
            ),
            rx.table.body(
                rx.cond(
                    NewLinksState.loading,
                    rx.table.row(
                        rx.table.cell(
                            "Loading...",
                            col_span=11,
                            style={"textAlign": "center", "fontStyle": "italic"},
                        )
                    ),
                    rx.cond(
                        NewLinksState.links.length() == 0,
                        rx.table.row(
                            rx.table.cell(
                                "No data available.",
                                col_span=11,
                                style={"textAlign": "center", "fontStyle": "italic"},
                            )
                        ),
                        rx.foreach(
                            NewLinksState.links,
                            lambda row: rx.table.row(
                                rx.table.cell(row[0]),
                                rx.table.cell(rx.badge(rx.link(row[1], href="https://bgp.he.net/AS" + row[1]))),
                                rx.table.cell(rx.badge(rx.link(row[2], href="https://bgp.he.net/AS" + row[2]))),

                                rx.table.cell(
                                    rx.foreach(
                                        row[3],
                                        lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x)),
                                    )
                                ),

                                rx.table.cell(
                                    rx.cond(
                                        row[4].length() > 10,
                                        # 👇 Special box with hover for long lists
                                        rx.hover_card.root(
                                            rx.hover_card.trigger(
                                                rx.badge(
                                                    f"{row[4].length()} victims",
                                                    color_scheme="red",
                                                    variant="surface",
                                                    style={"cursor": "pointer"},
                                                )
                                            ),
                                            rx.hover_card.content(
                                                rx.box(
                                                    rx.foreach(
                                                        row[4],
                                                        lambda x: rx.badge(
                                                            rx.link(x, href="https://bgp.he.net/AS" + x),
                                                            style={"margin": "0.1em"},
                                                        ),
                                                    ),
                                                    style={
                                                        "display": "flex",
                                                        "flexWrap": "wrap",
                                                        "gap": "0.25em",
                                                        "maxWidth": "300px",  # keeps popup tidy
                                                    },
                                                )
                                            ),
                                        ),
                                        # 👇 Default: inline list when ≤ 10 victims
                                        rx.box(
                                            rx.foreach(
                                                row[4],
                                                lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x)),
                                            ),
                                            style={"display": "flex", "flexWrap": "wrap", "gap": "0.25em"},
                                        ),
                                    )
                                ),

                                rx.table.cell(
                                    row[5],
                                    style=rx.cond(
                                        row[5] == "legitimate",
                                        {"backgroundColor": "#dcfce7"},   # green-100
                                        {"backgroundColor": "#fee2e2"},   # red-100
                                    ),
                                ),
                                rx.table.cell(row[6]),
                                rx.table.cell(row[7]),

                                rx.table.cell(
                                    rx.cond(
                                        row[8],
                                        rx.text("Yes"),
                                        rx.text("No"),
                                    )
                                ),
                            )
                        ),
                    ),
                )
            ),
        ),
        width="100%",
        style={"overflow_x": "auto"},
    )


# ---- PAGE ----
@rx.page(on_load=[NewLinksState.load_links])
def index() -> rx.Component:
    return rx.box(
        rx.heading("New BGP Links (Debug Mode)", size="6", margin_bottom="1em"),
        new_links_table(),
        width="100%",
        padding="2em",
    )


# ---- APP ----
app = rx.App()
app.add_page(index, title="New BGP Links")
