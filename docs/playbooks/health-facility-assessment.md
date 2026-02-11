# Pilot Playbook: Health Facility Assessment

Goal: quick facility readiness snapshot for clinics or health centers.

## Prebuilt project
- Name: Health Facility Assessment Pilot
- Status: Active
- Description: Facility-level readiness, staffing, and services.

## Template (Form) questions
Section: Facility profile
- Facility name (text, required)
- Facility type (dropdown: Hospital, Clinic, PHC, Other)
- Ownership (dropdown: Public, Private, Faith-based, NGO)
- Catchment population (number)

Section: Staffing
- Total clinical staff (number, required)
- Total non-clinical staff (number)
- Has at least one licensed clinician on-site? (yes/no, required)

Section: Services & supplies
- Essential drug stock available today? (yes/no, required)
- Basic lab available? (yes/no)
- Electricity available now? (yes/no, required)
- Water available now? (yes/no)

Section: Notes
- Key issues observed (long text)

## Suggested coverage structure
Facility-level capture with LGA and State roll-ups.

Recommended hierarchy:
State -> LGA -> Facility

## Enumerator instructions
- Start at assigned facility, confirm name and type
- Capture staffing and utilities in real time
- Add a short note for any blockers observed

## Enumerator assignment
- Enumerator name and optional code
- Optional coverage scope: facility or ward
- Use assignment-scoped share links

## Example exports
- docs/examples/health-facility-export.csv
- docs/examples/health-facility-export.json

## Success criteria
- 10-20 facilities completed
- Less than 5 percent missing required fields
- Supervisor export delivered to partner

## Outcome statement
“You can assess 100 facilities in 3 days.”
