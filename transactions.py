from kubernetes import client, config
import subprocess

# Global variable to store the sum of outputs
total_sum = 0

def get_ext_namespaces(v1):
    namespaces = v1.list_namespace()
    return [ns.metadata.name for ns in namespaces.items if ns.metadata.name.startswith('ext-')]

def copy_binary_to_pod(namespace, pod_name, local_binary_path):
    cmd = f"kubectl cp {local_binary_path} {namespace}/{pod_name}:get-katana-tx"
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"Successfully copied binary to {pod_name} in {namespace}")
    except subprocess.CalledProcessError as e:
        print(f"Error copying binary to {pod_name} in {namespace}: {e}")
        print(e.stderr)

def run_script_in_pod(namespace, pod_name, binary_args):
    global total_sum
    cmd = f"kubectl exec -n {namespace} {pod_name} -- ./get-katana-tx {binary_args}"
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"Successfully executed in {pod_name} of {namespace}")
        output = result.stdout.strip()
        if output.isdigit():
            output_value = int(output)
            total_sum += output_value
            print(f"Output: {output_value}")
        else:
            print(f"Error: Output is not a valid integer: {output}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing in {pod_name} of {namespace}: {e}")
        print(e.stderr)

def cleanup_pod(namespace, pod_name):
    binary_name = "get-katana-tx"
    # Check if the binary exists
    check_cmd = f"kubectl exec -n {namespace} {pod_name} -- ls {binary_name}"
    try:
        result = subprocess.run(check_cmd, shell=True, check=True, capture_output=True, text=True)
        # Assert that the output of ls is exactly the binary name we're expecting
        if result.stdout.strip() != binary_name:
            print(f"Unexpected file found in {pod_name} of {namespace}. Skipping cleanup.")
            return

        # If assertion passes, proceed with deletion
        delete_cmd = f"kubectl exec -n {namespace} {pod_name} -- rm -f {binary_name}"
        subprocess.run(delete_cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"Successfully cleaned up binary in {pod_name} of {namespace}")
    except subprocess.CalledProcessError as e:
        if "No such file or directory" in e.stderr:
            print(f"Binary not found in {pod_name} of {namespace}. No cleanup needed.")
        else:
            print(f"Error during cleanup in {pod_name} of {namespace}: {e}")
            print(e.stderr)

def main(local_binary_path, binary_args):
    config.load_kube_config()
    v1 = client.CoreV1Api()

    for namespace in get_ext_namespaces(v1):
        print(f"\nProcessing namespace: {namespace}")
        try:
            pods = v1.list_namespaced_pod(namespace)
            katana_pods = [pod for pod in pods.items if pod.metadata.name.startswith('katana-')]

            for pod in katana_pods:
                pod_name = pod.metadata.name
                print(f"Processing pod: {pod_name}")

                copy_binary_to_pod(namespace, pod_name, local_binary_path)
                run_script_in_pod(namespace, pod_name, binary_args)
                cleanup_pod(namespace, pod_name)

        except client.exceptions.ApiException as e:
            print(f"Error processing namespace {namespace}: {e}")

    print(f"\nTotal sum of all outputs: {total_sum}")

if __name__ == "__main__":
    local_binary_path = "./get-katana-tx"  # Assuming the binary is in the same directory as the script
    binary_args = "data"
    main(local_binary_path, binary_args)
