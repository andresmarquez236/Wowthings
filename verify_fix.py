
import json
import sys
import os

# Add the project root to sys.path
sys.path.append("/Users/andresmarquez/Documents/Infinitylab/wow_agent")

try:
    from shopify.mapper import map_payload_to_shopify_structure
    print("✅ Successfully imported map_payload_to_shopify_structure")
except ImportError as e:
    print(f"❌ Failed to import map_payload_to_shopify_structure: {e}")
    sys.exit(1)

# Mock data
shopify_base_json = {
    "sections": {
        "main": {
            "blocks": {
                "collapsible_tab_AUafHX": {"type": "collapsible_tab", "settings": {"content": ""}},
                "collapsible_tab_NKgDKr": {"type": "collapsible_tab", "settings": {"content": ""}},
                "c11bfb2f-901d-4b03-90cf-b6b766353d13": {"type": "collapsible_tab", "settings": {"content": ""}},
                "e6778313-45ad-4d6d-8260-2498242a6df0": {"type": "collapsible_tab", "settings": {"content": ""}}
            }
        }
    }
}

ai_content = {
    "extra_info_tabs": {
        "whats_included": "<ul><li>Item 1</li></ul>",
        "how_to_use": "<ol><li>Step 1</li></ol>",
        "shipping_info": "Free shipping.",
        "warranty_info": "30 days return."
    }
}

# Test mapping
try:
    final_json = map_payload_to_shopify_structure(shopify_base_json, ai_content)
    
    blocks = final_json['sections']['main']['blocks']
    
    assert blocks['collapsible_tab_AUafHX']['settings']['content'] == "<ul><li>Item 1</li></ul>"
    assert blocks['collapsible_tab_NKgDKr']['settings']['content'] == "<ol><li>Step 1</li></ol>"
    assert blocks['c11bfb2f-901d-4b03-90cf-b6b766353d13']['settings']['content'] == "Free shipping."
    assert blocks['e6778313-45ad-4d6d-8260-2498242a6df0']['settings']['content'] == "30 days return."
    
    print("✅ Mapping logic validated successfully.")
except Exception as e:
    print(f"❌ Mapping logic failed: {e}")
    sys.exit(1)
