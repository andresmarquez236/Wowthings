import sys
import os
sys.path.append(os.getcwd())

try:
    from shopify.image_landing_gen import section_social_proof, evaluator_benefits
    from shopify.upload_images import deploy_images
    from shopify.visual_plan.visual_planer import VisualPlaner
    from shopify.visual_plan.visual_injection import run_injection_pipeline
    from research.info_products import mark_landing_gen_completed
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

folder_name = "organizador_de_cuchillos_y_utensilios"
product_name = "Organizador De Cuchillos Y Utensilios"
row_idx = 11

print(f"--- RESUMING MANUAL PROCESSING FOR: {product_name} ---")

# 1. Social Proof (Resume)
try:
    print("\n[1/5] Resuming Social Proof...")
    section_social_proof.run_social_proof_pipeline(folder_name)
except Exception as e:
    print(f"Error in Social Proof: {e}")

# 2. Evaluator
try:
    print("\n[2/5] Running Evaluator...")
    evaluator_benefits.run_evaluation_pipeline(folder_name)
except Exception as e:
    print(f"Error in Evaluator: {e}")

# 3. Deploy
try:
    print("\n[3/5] Deploying Images...")
    deploy_images.deploy_pipeline(folder_name)
except Exception as e:
    print(f"Error in Deploy: {e}")

# 4. Visual Plan & Injection
print("\n[4/5] Visual Injection...")
try:
    planer = VisualPlaner()
    planer.analyze_and_generate(folder_name, product_name)
except Exception as e:
    print(f"Visual Planer error: {e}")

try:
    run_injection_pipeline(folder_name)
except Exception as e:
    print(f"Visual Injection error: {e}")

# 5. Mark Complete
try:
    print("\n[5/5] Marking as Complete...")
    mark_landing_gen_completed(row_idx)
except Exception as e:
    print(f"Error marking complete: {e}")

print(f"\n--- FINISHED PROCESSING {product_name} ---")
