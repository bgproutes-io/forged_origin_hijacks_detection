import reflex as rx
import requests
from components.navbar import navbar
from components.footer import footer
from typing import List, Tuple, Optional


# -----------------------------
# State for details page
# -----------------------------
class DetailState(rx.State):
    rows: List[Tuple[str, str, str, str, str, list[str], str, str, int, List[str], List[str]]] = []
    loading: bool = False
    error: Optional[str] = None

    as1: Optional[str] = None
    as2: Optional[str] = None

    @rx.event(background=True)
    async def load_details(self):
        async with self:
            self.loading = True
            self.error = None
        yield

        url = f"https://dfoh-api.bgproutes.io/inference_details?new_link_ids={self.case_id}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()["results"][self.case_id]  # expected: list of tuples
            else:
                data = []
        except Exception as e:
            print(f"Error fetching details: {e}")
            data = []

        # Parse rows
        parsed: List[
            Tuple[str, str, str, str, str, str, str, bool, int, List[str], List[str]]
        ] = []
        for tup in data:
            try:
                parsed.append((
                    tup[0].replace("T", " "),  # observed_at
                    str(tup[1]),  # asn1
                    str(tup[2]),  # asn2
                    str(tup[3]),  # peer_asn
                    str(tup[4]),  # peer_ip
                    [str(x) for x in tup[5].split(" ")],  # as_path
                    str(tup[6]),  # prefix
                    str(tup[7]),  # is_legit
                    int(tup[8]),  # confidence_level
                    [str(x) for x in tup[9]] if tup[9] else [],
                    [str(x) for x in tup[10]] if tup[10] else [],
                ))
            except Exception:
                continue

        # Set asn1/as2 from first row if available
        as1 = parsed[0][1] if parsed else None
        as2 = parsed[0][2] if parsed else None

        async with self:
            self.rows = parsed
            self.as1 = as1
            self.as2 = as2
            self.loading = False
        yield



def _badge_link(asn: str) -> rx.Component:
   return rx.badge(
       rx.link(asn, href="https://bgp.he.net/AS" + asn, target="_blank"),
           style={"verticalAlign": "middle"},
   )


def _aspath_cell(values: List[str]) -> rx.Component:
   return rx.table.cell(
       rx.box(
           rx.foreach(values, lambda x: rx.badge(rx.link(x, href="https://bgp.he.net/AS" + x, target="_blank"))),
           style={"display": "flex", "flexWrap": "wrap", "verticalAlign": "middle"},
       )
   )





# -----------------------------
# UI helpers
# -----------------------------
def _tags_box(tags: List[str]) -> rx.Component:
    return rx.box(
        rx.foreach(
            tags,
            lambda t: rx.badge(t, variant="soft"),
        ),
        style={"display": "flex", "flexWrap": "wrap", "gap": "0.25rem"},
    )


def details_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell("Observed at"),
                rx.table.column_header_cell("Peer ASN"),
                rx.table.column_header_cell("Peer IP"),
                rx.table.column_header_cell("AS Path"),
                rx.table.column_header_cell("Prefix"),
                rx.table.column_header_cell("Inference result"),
                rx.table.column_header_cell("Tags"),
            )
        ),
        rx.table.body(
            rx.cond(
                DetailState.loading,
                rx.table.row(
                    rx.table.cell(
                        "Loading…",
                        col_span=7,
                        style={"textAlign": "center", "fontStyle": "italic"},
                    )
                ),
                rx.cond(
                    DetailState.rows.length() == 0,
                    rx.table.row(
                        rx.table.cell(
                            "No details available.",
                            col_span=7,
                            style={"textAlign": "center", "fontStyle": "italic"},
                        )
                    ),
                    rx.foreach(
                        DetailState.rows,
                        lambda row: rx.table.row(
                            rx.table.cell(row[0], style={"verticalAlign": "middle"}),     # observed_at
                            rx.table.cell(_badge_link(row[3]), style={"verticalAlign": "middle"}),     # peer_asn
                            rx.table.cell(row[4], style={"verticalAlign": "middle"}),     # peer_ip
                            _aspath_cell(row[5]),
                            rx.table.cell(row[6], style={"verticalAlign": "middle"}),     # prefix
                            rx.table.cell(             # inference result with colored background
                                row[7],
                                style=rx.cond(
                                    row[7] == "legitimate",
                                    {
                                        "backgroundColor": "var(--green-3)",
                                        "verticalAlign": "middle",
                                    },
                                    rx.cond(
                                        row[7] == "suspicious",
                                        {
                                            "backgroundColor": "var(--red-3)",
                                            "verticalAlign": "middle",
                                        },
                                        {"verticalAlign": "middle"},
                                    ),
                                ),
                            ),
                            rx.table.cell(_tags_box(row[10]), style={"verticalAlign": "middle"}),  # pfx_tags
                        ),
                    ),
                ),
            )
        ),
        size="3",
        variant="surface",
        style={"--table-row-hover-bg": "var(--gray-2)"},
    )


# -----------------------------
# Page
# -----------------------------
@rx.page(route="/detail/[case_id]", on_load=[DetailState.load_details])
def detail_page() -> rx.Component:
    return rx.fragment(
        navbar(),
        rx.box(
            # --- Page content ---
            rx.heading("Case Summary", size="7", mb="4"),
            rx.cond(
                DetailState.as1 != None,
                rx.hstack(
                    rx.link(
                        rx.text(f"AS{DetailState.as1}", size="6", weight="bold"),
                        href=f"https://bgp.he.net/AS{DetailState.as1}",
                        target="_blank",
                        underline="always",
                    ),
                    rx.text("—", size="6", weight="bold"),
                    rx.link(
                        rx.text(f"AS{DetailState.as2}", size="6", weight="bold"),
                        href=f"https://bgp.he.net/AS{DetailState.as2}",
                        target="_blank",
                        underline="always",
                    ),
                    spacing="4",
                    align="center",
                    mb="6",
                ),
                rx.text("Loading case summary...", size="5", mb="4"),
            ),

            rx.box(height="1.25rem"),

            details_table(),
            width="100%",
            padding_x=["1rem","2rem","3rem","3rem"],
            padding_y="2rem",
            margin_top="5rem",   # 👈 push content below fixed navbar
            style={"maxWidth": "1600px", "margin": "0 auto"},
        ),
        footer(),
    )