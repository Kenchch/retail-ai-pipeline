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
    # Whitespace-normalised and matched with its "equals", rather than by the
    # punctuation that happens to follow. This assertion read `f"£{net}."` and
    # broke when the sentence gained a clause after the figure -- the number
    # was right and the test failed on a full stop.
    net = c["net_of_matched_cancellations_gbp"]
    assert f"equals £{net:,.2f}" in " ".join(design.split())


def test_the_reconciliation_with_r_states_this_pipelines_own_figures():
    """The table claims the two projects agree, so both columns are this
    pipeline's numbers written twice -- and the ones in this pipeline's column
    have to be the ones it actually produced.

    This used to assert a *gap*, computed from a sentence that read "R's X
    differs by Y". There is no gap now; what needs guarding instead is that the
    agreement is stated with current figures rather than frozen ones, because a
    table asserting equality is exactly where a stale number is hardest to see.
    """
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    design = _design()

    rows = {
        "Gross positive product sales": c["gross_gbp"],
        "Value removed by matched credit notes": c["matched_credit_gbp"],
    }
    for label, value in rows.items():
        assert f"| {label} | £{value:,.2f} | £{value:,.2f} |" in design, label

    net = c["net_of_matched_cancellations_gbp"]
    assert (
        f"| **Net of matched cancellations** | **£{net:,.2f}** | **£{net:,.2f}** |"
        in (design)
    )
    # And the row is a subtraction, performed rather than trusted.
    assert abs(c["gross_gbp"] - c["matched_credit_gbp"] - net) < 0.005


def test_the_matched_count_difference_against_r_is_stated_with_our_own_number():
    """R matches more rows than this pipeline and the totals still agree. The
    explanation is load-bearing -- without it the two counts read as a bug --
    so this pipeline's half of it cannot go stale."""
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    ours = c["matched_accepted_sales_rows"]
    theirs = 2769  # R's, from its committed cleaning_audit.csv
    # Whitespace-normalised: the sentence wraps, and where it wraps is not a
    # fact worth pinning into a test.
    design = " ".join(_design().split())

    # BOTH times, counted. The comparison is made twice in this document, and
    # an `in` check was satisfied by either one -- so changing the figure in
    # the second passage left the test green while the two passages disagreed
    # with each other.
    assert design.count(f"against this pipeline's {ours:,}") == 2
    assert design.count(f"R records {theirs:,}") == 1
    assert design.count(f"matched count is {theirs:,}") == 1
    # The 44 is the subtraction, not a third number.
    assert design.count(f"The {theirs - ours} are service-code lines") == 1
    assert design.count(f"the {theirs - ours} are service-code lines") == 1


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
