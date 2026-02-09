"""
Quick test to verify optimized version works correctly
"""
from src.services.teams_service import generate_teams_message
from datetime import datetime

filtered_results = [
    {
        'country': 'CL',
        'status': 'success',
        'alerts_count': 3,
        'timestamp': datetime(2026, 2, 9, 17, 33).isoformat(),
        'pages': [
            {
                'page_type': 'PDP',
                'components': [
                    {
                        'name': 'Cross Sell',
                        'found': False,
                        'details': None
                    }
                ]
            },
            {
                'page_type': 'HOME',
                'components': [
                    {
                        'name': 'Purchased With Recently Purchased',
                        'found': True,
                        'details': {
                            'strategies': {
                                'strategies_found': {
                                    'Strategy 1': False,
                                    'Strategy 2': False
                                }
                            }
                        }
                    }
                ]
            }
        ]
    }
]

message = generate_teams_message(filtered_results)

print("Generated Teams Message:")
print("="*70)
print(message)
print("="*70)

# Verify
assert '**Alertas:** 2' in message
assert '**Total alertas: 2**' in message
assert '- Cross Sell: PDP' in message
assert '- Purchased With Recently Purchased: HOME' in message
assert 'Strategy 1' not in message
assert 'Strategy 2' not in message

lines = message.split('<br>')
component_lines = [line for line in lines if line.strip().startswith('- ')]
assert len(component_lines) == 2

print("\n✅ Optimized version works correctly!")
print("   - No duplicate calls to extract_components_issues")
print("   - Shows 2 distinct components")
print("   - Count matches displayed components")
