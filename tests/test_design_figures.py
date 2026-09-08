"""Numbers in DESIGN.md and bi/README.md, checked against what produces them.

`test_doc_figures.py` covers the two audience documents. DESIGN.md was not
covered by anything, and it had drifted in five places at once: a runtime of
9.3 s against a run that took 14.1, one contracted mart against four, four
modules against seven, "proves that the DAG parses" after CI started executing
it, and a 35-measure DAX library against 37 measures.

None of those is a typo. Each was true when written and was falsified by a
later change to the thing it describes -- which is the only failure mode worth
a test here, and the reason each assertion below reads its number from the
artefact rather than from a second copy of the prose.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _design() -> str:
    return (ROOT / "docs" / "DESIGN.md").read_text(encoding="utf-8")


def test_runtime_matches_the_published_run():
    metrics = json.loads(
        (ROOT / "reports/run_metrics.json").read_text(encoding="utf-8")
    )
    assert f"{metrics['compute_seconds']} s of compute" in _design()


def test_headline_row_counts_match_the_published_run():
    """Four counts in one table row, all from the same run."""
    m = json.loads((ROOT / "reports/run_metrics.json").read_text(encoding="utf-8"))
    design = _design()
    assert f"**{m['rows_quarantined']:,} ({m['quarantine_rate_pct']}%)**" in design
    assert f"{m['rows_loaded']:,} line items" in design
    assert f"{m['products']:,} products" in design
    assert f"{m['recommendations']:,} rows covering the full catalogue" in design


def test_credit_matching_figures_match_their_report():
    """Five numbers in one paragraph, all from one report, all of which move
    together whenever the matching policy changes -- as they did when the
    credit side started deduplicating by the quality rule's key."""
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    design = _design()
    assert (
        f"matches {c['matched_accepted_sales_rows']:,} accepted sales "
        f"({c['matched_share_of_all_credit_rows_pct']}% of all "
        f"{c['credit_note_rows']:,}" in design
    )
    assert f"£{c['gross_gbp']:,.2f} gross" in design
    assert f"minus £{c['matched_credit_gbp']:,.2f} matched value" in design
    # The line wraps between "equals" and the figure, so match the figure
    # alone rather than pinning the document's line breaks into a test.
    net = c["net_of_matched_cancellations_gbp"]
    assert f"£{net:,.2f}." in design


def test_the_gap_against_the_r_analysis_is_arithmetic_not_recollection():
    """The comparison is the point of quoting R's figure at all. It is stated
    as a subtraction, so it has to survive being performed."""
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    design = _design()
    match = re.search(r"R's £([\d,]+\.\d\d) differs by £([\d,]+\.\d\d)", design)
    assert match, "the R comparison sentence is no longer in the shape this test reads"
    r_net = float(match.group(1).replace(",", ""))
    stated_gap = float(match.group(2).replace(",", ""))
    actual = r_net - c["net_of_matched_cancellations_gbp"]
    assert abs(actual - stated_gap) < 0.005, (
        f"the stated gap is £{stated_gap:,.2f}; £{r_net:,.2f} minus the current "
        f"net £{c['net_of_matched_cancellations_gbp']:,.2f} is £{actual:,.2f}"
    )


def test_module_count_matches_the_package():
    """Adding a stage module without saying so is exactly how "Four" survived
    into a package of seven."""
    modules = sorted(
        p.stem for p in (ROOT / "retail_pipeline").glob("*.py") if p.stem != "__init__"
    )
    words = {4: "Four", 5: "Five", 6: "Six", 7: "Seven", 8: "Eight", 9: "Nine"}
    assert f"{words[len(modules)]} modules, scheduled as" in _design(), (
        f"the package holds {len(modules)} modules: {modules}"
    )


def test_task_count_matches_the_dag_source():
    """Read from the DAG file rather than by importing it: Airflow is not in
    the base requirements, so this test has to run in the default job too."""
    source = (ROOT / "dags/retail_pipeline_dag.py").read_text(encoding="utf-8")
    tasks = re.findall(r'task_id="([^"]+)"', source)
    words = {10: "ten", 11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen"}
    assert f"scheduled as {words[len(tasks)]} Airflow tasks" in _design(), (
        f"the DAG declares {len(tasks)} tasks: {tasks}"
    )


def test_contracted_marts_are_named_and_counted():
    """The contract is the claim, so it is read from the dbt schema: a mart
    listed here without `contract:` would make the sentence false."""
    import yaml

    schema = yaml.safe_load(
        (ROOT / "dbt/models/marts/schema.yml").read_text(encoding="utf-8")
    )
    contracted = [
        model["name"]
        for model in schema["models"]
        if model.get("config", {}).get("contract", {}).get("enforced")
    ]
    words = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}
    design = _design()
    claim = f"builds {words[len(contracted)]} contracted marts"
    assert claim in design, f"dbt enforces contracts on {len(contracted)}: {contracted}"

    # Within the sentence that makes the claim, not merely somewhere in the
    # document: every one of these names appears again in the model table
    # further down, so a name dropped from the list here would still be found.
    start = design.index(claim)
    sentence = design[start : design.index(".", start) + 1]
    unnamed = [name for name in contracted if f"`{name}`" not in sentence]
    assert not unnamed, f"{unnamed} has an enforced contract and the claim omits it"


def test_dax_measure_count_matches_the_library():
    """Counted the way the file is written: a measure is `Name =` on its own
    line, and section 8 is role definitions, which the file says are not
    measures. VAR lines are locals inside a measure body."""
    lines = (ROOT / "bi/measures.dax").read_text(encoding="utf-8").splitlines()
    cut = next(i for i, line in enumerate(lines) if line.startswith("// 8. Row-level"))
    measures = [
        m.group(1).strip()
        for line in lines[:cut]
        if (m := re.match(r"^([A-Za-z][^=]*?)\s*=\s*$", line))
        and not line.startswith("VAR ")
    ]
    assert len(measures) == len(set(measures)), "two measures share a name"
    count = len(measures)
    assert f"{count}-measure DAX library" in _design()
    bi_readme = (ROOT / "bi/README.md").read_text(encoding="utf-8")
    assert f"a {count}-measure DAX library" in bi_readme
    assert f"| {count} measures and the three RLS role expressions |" in bi_readme


def test_the_design_document_does_not_still_claim_ci_only_parses():
    """CI executes the DAG. The sentence that said otherwise outlived it by a
    release, and the replacement is worth pinning: "parses" is a weaker claim
    that would read as true to anyone not checking the workflow."""
    design = _design()
    assert "proves that\nthe DAG parses" not in design
    assert "executes the\nDAG end to end with `dag.test()`" in design
