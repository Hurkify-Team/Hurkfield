# Pilot Playbook: NGO Baseline & Endline Survey

Goal: baseline household or community survey before program start.

## Prebuilt project
- Name: NGO Baseline Pilot
- Status: Active
- Description: Baseline indicators for program planning.

## Templates (Baseline + Endline)
Baseline and Endline templates are both included.

Baseline sections:
- Respondent: name, age, gender (consent required)
- Household: size, livelihood, water access/source
- Program indicators: schooling, health facility access, income
- Notes: enumerator observations

Endline sections:
- Respondent: name, age, gender (consent required)
- Household: size, livelihood, water access/source
- Program outcomes: indicators + what improved + what remains a challenge
- Notes: enumerator observations

Consent is collected via the built-in ethics prompt (required).

## Suggested coverage structure
Country -> State -> LGA -> Community -> Household

## Enumerator assignment
- Enumerator name and optional code
- Optional coverage scope: community or household
- Use assignment-scoped share links

## Draft / resume flow
- Drafts are available on-device by default
- Enable server drafts with OPENFIELD_SERVER_DRAFTS=true for resume links

## Enumerator performance tracking
- Use Project Analytics to track submissions, drafts, and QA flags by enumerator

## QA alert examples
- Missing consent or required household size
- Duplicate submissions for the same household
- GPS missing when required

## Example exports
Baseline:
- docs/examples/ngo-baseline-export.csv
- docs/examples/ngo-baseline-export.json
Endline:
- docs/examples/ngo-endline-export.csv
- docs/examples/ngo-endline-export.json

## Success criteria
- 30-50 submissions
- QA flags logged and resolved
- Export shared with program lead

## Outcome statement
“Measure impact without rebuilding tools.”
