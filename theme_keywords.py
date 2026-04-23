"""
Theme Keywords for GEV EHS Platform
Mirrors the JavaScript classification logic for client and server-side theme detection.
"""

THEMES = [
    'Safety Culture',
    'Hazard Communication',
    'Personal Protective Equipment',
    'Incident Reporting',
    'Inspection & Monitoring',
    'Leadership Engagement',
    'Contractor Management',
    'Chemical Management',
    'Ergonomics',
    'Fire Safety',
    'Environmental',
    'Machine Guarding',
    'Fall Protection',
    'Process Safety',
    'Training Competency',
    'Health & Wellness',
    'Supply Chain Safety',
    'Audit & Compliance',
    'Site Readiness',
    'Asset Maintenance',
    'System Resilience'
]

THEME_KEYWORDS = {
    'Safety Culture': r'culture|attitude|mindset|engagement|ownership|empowerment',
    'Hazard Communication': r'hazard|label|sds|chemical|symbol|warning|communication',
    'Personal Protective Equipment': r'ppe|helmet|glove|glasses|respirator|hard hat|safety vest',
    'Incident Reporting': r'report|incident|near miss|accident|injury|investigation',
    'Inspection & Monitoring': r'inspection|audit|observation|assessment|review|monitoring',
    'Leadership Engagement': r'leadership|supervisor|manager|commitment|accountability',
    'Contractor Management': r'contractor|vendor|third party|subcontractor|outsource',
    'Chemical Management': r'chemical|substance|hazardous|material|toxic|chemical handling',
    'Ergonomics': r'ergonomic|posture|repetitive|strain|lifting|back|joint',
    'Fire Safety': r'fire|burn|combustible|ignition|flame|emergency evacuation',
    'Environmental': r'environmental|emission|waste|water|contamination|pollution',
    'Machine Guarding': r'machine|guarding|lockout|tagout|loto|equipment safety',
    'Fall Protection': r'fall|height|scaffold|ladder|harness|fall arrest',
    'Process Safety': r'process|critical|safety system|failure|risk assessment',
    'Training Competency': r'training|competency|certification|qualification|skill',
    'Health & Wellness': r'health|wellness|disease|illness|mental|occupational illness',
    'Supply Chain Safety': r'supplier|supply chain|sourcing|procurement|vendor management',
    'Audit & Compliance': r'audit|compliance|regulation|law|regulatory|standard',
    'Site Readiness': r'readiness|preparation|planning|procedure|protocol',
    'Asset Maintenance': r'maintenance|preventive|predictive|repair|upkeep',
    'System Resilience': r'resilience|redundancy|backup|contingency|continuity'
}
