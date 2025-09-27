import reflex as rx
from components.navbar import navbar
from components.footer import footer
import requests
from typing import List, Tuple, Optional


# -----------------------------
# Helpers / validation
# -----------------------------
def case_is_correct(case_: dict[str, str | int | list[str]]):
   required = [
       "date",
       "as1",
       "as2",
       "presumed_attacker",
       "presumed_victims",
       "inference_result",
       "confidence_level",
       "nb_aspaths_observed",
       "is_reccurent",
       "id",
   ]
   for k in required:
       if k not in case_:
           return False, None

   return True, (
       case_["date"].replace("T", " "),                         # 0 date as str "YYYY-MM-DD HH:MM:SS"
       str(case_["as1"]),                                       # 1 as1
       str(case_["as2"]),                                       # 2 as2
       [str(x) for x in case_["presumed_attacker"]],            # 3 attackers list[str]
       [str(x) for x in case_["presumed_victims"]],             # 4 victims list[str]
       str(case_["inference_result"]),                          # 5 inference_result
       int(case_["confidence_level"]),                          # 6 confidence (int)
       int(case_["nb_aspaths_observed"]),                       # 7 nb paths (int)
       bool(case_["is_reccurent"]),                             # 8 recurrent (bool)
       int(case_["id"])
   )



# -----------------------------
# STATE
# -----------------------------
class NewLinksState(rx.State):
    links: List[Tuple[str, str, str, List[str], List[str], str, int, int, bool, int]] = []
    loading: bool = False
    error: Optional[str] = None

    # --- Filters ---
    start_dt_local: str = ""
    end_dt_local: str = ""
    as1_filter: str = ""
    as2_filter: str = ""
    attacker_filter: str = ""
    victim_filter: str = ""
    inference_filter: str = "Any"
    confidence_min: str = ""
    confidence_max: str = ""
    recurrent_filter: str = "Any"
    hide_private_asn: bool = True   # ✅ default: filter out private ASNs

    # --- Pagination ---
    page_number: int = 1
    rows_per_page: int = 50

    # --------------------
    # Data loading
    # --------------------
    @rx.event(background=True)
    async def load_links(self):
        async with self:
            self.loading = True
            self.error = None
        yield

        try:
            # --- Build query parameters from filters ---
            params = {}

            if self.start_dt_local:
                params["start"] = self.start_dt_local
            if self.end_dt_local:
                params["end"] = self.end_dt_local
            if self.as1_filter:
                params["asn"] = self.as1_filter
            if self.as2_filter:
                params["as2"] = self.as2_filter
            if self.attacker_filter:
                params["attacker"] = self.attacker_filter
            if self.victim_filter:
                params["victim"] = self.victim_filter
            if self.inference_filter and self.inference_filter != "Any":
                params["inference_result"] = self.inference_filter
            if self.confidence_min:
                params["conf_min"] = self.confidence_min
            if self.confidence_max:
                params["conf_max"] = self.confidence_max
            if self.recurrent_filter and self.recurrent_filter != "Any":
                params["recurrent"] = self.recurrent_filter

            params["show_private_asn"] = "false" if self.hide_private_asn else "true"

            # --- Call API with parameters ---
            resp = requests.get(
                "https://dfoh-api.bgproutes.io/new_links",
                params=params,
                timeout=10,
            )
            data = resp.json().get("results", []) if resp.status_code == 200 else []
        except Exception as e:
            print(f"Error fetching data: {e}")
            data = []

        parsed: List[Tuple[str, str, str, List[str], List[str], str, int, int, bool]] = []
        for case_ in data:
            ok, val = case_is_correct(case_)
            if ok and val is not None:
                parsed.append(val)

        parsed.sort(key=lambda x: x[0], reverse=True)  # newest first

        async with self:
            self.links = parsed
            self.loading = False
            self.page_number = 1  # reset to first page on reload
        yield

    # --------------------
    # Filter setters
    # --------------------
    @rx.event
    def set_start(self, v: str): self.start_dt_local = v

    @rx.event
    def set_end(self, v: str): self.end_dt_local = v

    @rx.event
    def set_as1(self, v: str): self.as1_filter = v.strip()

    @rx.event
    def set_as2(self, v: str): self.as2_filter = v.strip()

    @rx.event
    def set_attacker(self, v: str): self.attacker_filter = v

    @rx.event
    def set_victim(self, v: str): self.victim_filter = v

    @rx.event
    def set_inference(self, v: str): self.inference_filter = v

    @rx.event
    def set_confmin(self, v: str): self.confidence_min = v.strip()

    @rx.event
    def set_confmax(self, v: str): self.confidence_max = v.strip()

    @rx.event
    def set_recurrent(self, v: str): self.recurrent_filter = v

    @rx.event
    def toggle_hide_private_asn(self, value: bool):
        self.hide_private_asn = value

    @rx.event
    def reset_filters(self):
        self.start_dt_local = ""
        self.end_dt_local = ""
        self.as1_filter = ""
        self.as2_filter = ""
        self.attacker_filter = ""
        self.victim_filter = ""
        self.inference_filter = "Any"
        self.confidence_min = ""
        self.confidence_max = ""
        self.recurrent_filter = "Any"
        self.hide_private_asn = False
        self.page_number = 1


    # --------------------
    # Pagination helpers
    # --------------------
    @rx.var
    def total_pages(self) -> int:
        if not self.links:
            return 1
        return (len(self.links) + self.rows_per_page - 1) // self.rows_per_page

    @rx.var
    def get_current_page(self) -> List[Tuple[str, str, str, List[str], List[str], str, int, int, bool]]:
        start = (self.page_number - 1) * self.rows_per_page
        end = start + self.rows_per_page
        return self.links[start:end]

    @rx.event
    def first_page(self): self.page_number = 1

    @rx.event
    def last_page(self): self.page_number = self.total_pages

    @rx.event
    def next_page(self):
        if self.page_number < self.total_pages:
            self.page_number += 1

    @rx.event
    def prev_page(self):
        if self.page_number > 1:
            self.page_number -= 1

    @rx.event
    def set_page(self, n: int):
        if 1 <= n <= self.total_pages:
            self.page_number = n


# -----------------------------
# UI bits
# -----------------------------
def _badge_link(asn: str) -> rx.Component:
   return rx.badge(
       rx.link(asn, href="https://bgp.he.net/AS" + asn, target="_blank"),
           style={"verticalAlign": "middle"},
   )


def _attacker_cell(values: List[str]) -> rx.Component:
   return rx.table.cell(
       rx.box(
           rx.foreach(values, lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x, target="_blank"))),
           style={"display": "flex", "flexWrap": "wrap"},
       ),
        style={"verticalAlign": "middle"}
   )


def _victim_cell(values: List[str]) -> rx.Component:
   return rx.table.cell(
       rx.cond(
           values.length() > 6,
           rx.hover_card.root(
               rx.hover_card.trigger(
                   rx.badge(f"{values.length()} victims", variant="surface", style={"cursor": "pointer"})
               ),
               rx.hover_card.content(
                   rx.box(
                       rx.foreach(
                           values,
                           lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x, target="_blank"))),
                       style={"display": "flex", "flexWrap": "wrap", "maxWidth": "360px"},
                   )
               ),
           ),
           rx.box(
               rx.foreach(values, lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x, target="_blank"))),
               style={"display": "flex", "flexWrap": "wrap"},
           ),
       ),
       style={"verticalAlign": "middle"}
   )


def filters_panel() -> rx.Component:
    return rx.card(
        rx.form.root(  # ✅ make this a form so "submit" works naturally
            rx.vstack(
                # --- Always visible filters ---
                rx.hstack(
                    # ASN
                    rx.box(
                        rx.text("ASN", size="2", weight="medium"),
                        rx.input(
                            placeholder="e.g., 15169",
                            value=NewLinksState.as1_filter,
                            on_change=NewLinksState.set_as1,
                            name="asn",
                            width="100%",
                        ),
                        display="flex",
                        flex_direction="column",
                        gap="1",
                        width="30%",
                    ),
                    # Start Date
                    rx.box(
                        rx.text("Start (UTC)", size="2", weight="medium"),
                        rx.input(
                            type="datetime-local",
                            value=NewLinksState.start_dt_local,
                            on_change=NewLinksState.set_start,
                            name="start_dt",
                            width="100%",
                        ),
                        display="flex",
                        flex_direction="column",
                        gap="1",
                        width="30%",
                    ),
                    # End Date
                    rx.box(
                        rx.text("End (UTC)", size="2", weight="medium"),
                        rx.input(
                            type="datetime-local",
                            value=NewLinksState.end_dt_local,
                            on_change=NewLinksState.set_end,
                            name="end_dt",
                            width="100%",
                        ),
                        display="flex",
                        flex_direction="column",
                        gap="1",
                        width="30%",
                    ),
                    spacing="4",
                    width="100%",
                    justify="start",
                ),

                # --- Expandable advanced filters ---
                # --- Expandable advanced filters ---
                rx.accordion.root(
                    rx.accordion.item(
                        header="More filters...",
                        content=rx.box(   # <-- wrap in box so we can style the content
                            rx.grid(
                                # Attackers
                                rx.box(
                                    rx.text("Presumed attacker(s)", size="2", weight="medium"),
                                    rx.input(
                                        placeholder="Comma-separated (e.g., 64512,64496)",
                                        value=NewLinksState.attacker_filter,
                                        on_change=NewLinksState.set_attacker,
                                        name="attackers",
                                        width="100%",
                                    ),
                                    display="flex",
                                    flex_direction="column",
                                    gap="1",
                                ),
                                # Victims
                                rx.box(
                                    rx.text("Presumed victim(s)", size="2", weight="medium"),
                                    rx.input(
                                        placeholder="Comma-separated (e.g., 64512,64496)",
                                        value=NewLinksState.victim_filter,
                                        on_change=NewLinksState.set_victim,
                                        name="victims",
                                        width="100%",
                                    ),
                                    display="flex",
                                    flex_direction="column",
                                    gap="1",
                                ),
                                # Inference
                                rx.box(
                                    rx.text("Inference result", size="2", weight="medium"),
                                    rx.select(
                                        items=["Any", "legitimate", "suspicious"],
                                        value=NewLinksState.inference_filter,
                                        on_change=NewLinksState.set_inference,
                                        name="inference",
                                        width="100%",
                                    ),
                                    display="flex",
                                    flex_direction="column",
                                    gap="1",
                                ),
                                # Recurrent
                                rx.box(
                                    rx.text("Recurrent", size="2", weight="medium"),
                                    rx.select(
                                        items=["Any", "Yes", "No"],
                                        value=NewLinksState.recurrent_filter,
                                        on_change=NewLinksState.set_recurrent,
                                        name="recurrent",
                                        width="100%",
                                    ),
                                    display="flex",
                                    flex_direction="column",
                                    gap="1",
                                ),
                                rx.hstack(
                                    rx.checkbox(
                                        "Hide private ASNs",
                                        checked=NewLinksState.hide_private_asn,
                                        on_change=NewLinksState.toggle_hide_private_asn,
                                    ),
                                    spacing="2",
                                    align="center",
                                ),
                                columns="2",
                                spacing="4",
                                width="100%",
                            ),
                        ),
                        type="single",
                        variant="soft",   # affects the header button
                        width="100%",
                    ),
                    type="single",
                    collapsible=True,
                    width="100%",
                    variant="soft",   # affects the header
                ),

                # --- Buttons + results counter ---
                rx.hstack(
                    rx.button(
                        "Reset filters",
                        on_click=NewLinksState.reset_filters,
                        type="button",  # don’t trigger form submit
                        variant="soft",
                        size="2",
                        left_icon="rotate-ccw",
                    ),
                    rx.button(
                        "Submit",
                        type="submit",  # ✅ will call on_submit
                        variant="solid",
                        size="2",
                        left_icon="play",
                    ),
                    rx.spacer(),
                    rx.badge(
                        rx.text(
                            rx.cond(
                                NewLinksState.links.length() == NewLinksState.links.length(),
                                f"Showing all {NewLinksState.links.length()} results",
                                f"Showing {NewLinksState.links.length()} of {NewLinksState.links.length()} results",
                            )
                        ),
                        variant="soft",
                    ),
                    width="100%",
                    align="center",
                    spacing="3",
                    mt="3",
                ),
            ),
            on_submit=NewLinksState.load_links,  # ✅ submit refetches data
            width="100%",
        ),
        style={
            "border": "1px solid var(--gray-5)",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.05)",
        },
    )


def _pagination_view() -> rx.Component:
    return rx.hstack(
        rx.text(
            "Page ",
            rx.code(NewLinksState.page_number),
            f" of {NewLinksState.total_pages}",
            justify="end",
        ),
        rx.hstack(
            rx.icon_button(
                rx.icon("chevrons-left", size=18),
                on_click=NewLinksState.first_page,
                opacity=rx.cond(NewLinksState.page_number == 1, 0.6, 1),
                color_scheme=rx.cond(NewLinksState.page_number == 1, "gray", "accent"),
                variant="soft",
            ),
            rx.icon_button(
                rx.icon("chevron-left", size=18),
                on_click=NewLinksState.prev_page,
                opacity=rx.cond(NewLinksState.page_number == 1, 0.6, 1),
                color_scheme=rx.cond(NewLinksState.page_number == 1, "gray", "accent"),
                variant="soft",
            ),
            rx.icon_button(
                rx.icon("chevron-right", size=18),
                on_click=NewLinksState.next_page,
                opacity=rx.cond(
                    NewLinksState.page_number == NewLinksState.total_pages, 0.6, 1
                ),
                color_scheme=rx.cond(
                    NewLinksState.page_number == NewLinksState.total_pages,
                    "gray",
                    "accent",
                ),
                variant="soft",
            ),
            rx.icon_button(
                rx.icon("chevrons-right", size=18),
                on_click=NewLinksState.last_page,
                opacity=rx.cond(
                    NewLinksState.page_number == NewLinksState.total_pages, 0.6, 1
                ),
                color_scheme=rx.cond(
                    NewLinksState.page_number == NewLinksState.total_pages,
                    "gray",
                    "accent",
                ),
                variant="soft",
            ),
            align="center",
            spacing="2",
            justify="end",
        ),
        spacing="5",
        margin_top="1em",
        align="center",
        width="100%",
        justify="end",
    )


def new_links_table() -> rx.Component:
    return rx.card(
        _pagination_view(),   # ✅ pagination under the table

        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Date", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("AS1", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("AS2", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Presumed Attacker", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Presumed Victims", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Inference", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Observed Paths", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Recurrent", style={"verticalAlign": "middle"}),
                    rx.table.column_header_cell("Details", style={"verticalAlign": "middle"}),
                ),
                style={
                    "position": "sticky",
                    "top": 0,
                    "zIndex": 1,
                    "backgroundColor": "var(--gray-1)",
                    "backdropFilter": "blur(2px)",
                },
            ),
            rx.table.body(
                rx.cond(
                    NewLinksState.loading,
                    rx.table.row(
                        rx.table.cell(
                            "Loading…",
                            col_span=8,
                            style={"textAlign": "center", "fontStyle": "italic"},
                        )
                    ),
                    rx.cond(
                        NewLinksState.get_current_page.length() == 0,
                        rx.table.row(
                            rx.table.cell(
                                "No data matches your filters.",
                                col_span=8,
                                style={"textAlign": "center", "fontStyle": "italic"},
                            )
                        ),
                        rx.foreach(
                            NewLinksState.get_current_page,
                            lambda row: rx.table.row(
                                rx.table.cell(row[0], style={"verticalAlign": "middle"}),
                                rx.table.cell(_badge_link(row[1]), style={"verticalAlign": "middle"}),
                                rx.table.cell(_badge_link(row[2]), style={"verticalAlign": "middle"}),
                                _attacker_cell(row[3]),
                                _victim_cell(row[4]),
                                rx.table.cell(
                                    row[5],
                                    style=rx.cond(
                                        row[5] == "legitimate",
                                        {"verticalAlign": "middle", "backgroundColor": "var(--green-3)"},
                                        {"verticalAlign": "middle", "backgroundColor": "var(--red-3)"},
                                    ),
                                ),
                                rx.table.cell(row[7], style={"verticalAlign": "middle"}),
                                rx.table.cell(
                                    rx.cond(
                                        row[8],
                                        rx.text("Yes"),
                                        rx.text("No"),
                                    ),
                                    style={"verticalAlign": "middle"},
                                ),
                                # --- NEW Details button ---
                                rx.table.cell(
                                    rx.link(
                                        rx.button(
                                            "Details",
                                            size="1",
                                            variant="soft",
                                            left_icon="info",
                                        ),
                                        href=f"/detail/{row[9]}",
                                    ),
                                    style={"textAlign": "center", "verticalAlign": "middle"},
                                ),
                            ),
                        )
                    ),
                )
            ),
            size="3",
            variant="surface",
            style={
                "--table-row-hover-bg": "var(--gray-2)",
            },
        ),
        _pagination_view(),   # ✅ pagination under the table
        width="100%",
        style={
            "overflowX": "auto",
            "border": "1px solid var(--gray-5)",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.06)",
        },
    )




@rx.page(on_load=[NewLinksState.load_links], route="/")
def index() -> rx.Component:
    return rx.box(
        navbar(),
        rx.hstack(
            rx.box(
                rx.flex(
                    rx.icon("git-branch"),
                    rx.heading("New BGP Links", size="7"),
                    gap="2",
                    align="center",
                    mb="4",
                ),
                filters_panel(),
                rx.box(height="1.25rem"),
                new_links_table(),
                width="100%",
                padding_x=["1rem","2rem","3rem","3rem"],
                padding_y="2rem",
                margin_top="5rem",   # 👈 push content below fixed navbar
                style={"maxWidth": "1600px", "margin": "0 auto"},
            ),
            align="start",
            width="100%",
        ),
        footer(),
        width="100%",
    )