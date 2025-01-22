from kubernetes import client, config
import subprocess
import argparse
import os
import json

def get_ext_namespaces_and_pods():
    # load kube config
    config.load_kube_config()

    # create api client
    v1 = client.CoreV1Api()

    # get all namespaces and pods
    namespaces = []
    for ns in v1.list_namespace().items:
        if ns.metadata.name.startswith('ext-'):
            pods = v1.list_namespaced_pod(ns.metadata.name)
            # Filter pods starting with katana-
            filtered_pods = [pod for pod in pods.items if pod.metadata.name.startswith('katana-')]
            if filtered_pods:
                namespaces.append((ns.metadata.name, filtered_pods))

    return namespaces

def copy_and_execute_binary(pod, namespace, binary_path, dry_run=True):
    """Copy binary to pod and execute it"""
    commands = []

    # create temp directory in pod
    exec_command = [
        'kubectl', 'exec',
        pod.metadata.name,
        '-n', namespace,
        '--', 'mkdir', '-p', '/tmp/slot-scrapper'
    ]
    commands.append((' '.join(exec_command), "Create temp directory"))

    # copy binary to pod
    copy_command = [
        'kubectl', 'cp',
        binary_path,
        f"{namespace}/{pod.metadata.name}:/tmp/slot-scrapper/slot-scrapper"
    ]
    commands.append((' '.join(copy_command), "Copy binary to pod"))

    # make binary executable
    chmod_command = [
        'kubectl', 'exec',
        pod.metadata.name,
        '-n', namespace,
        '--',
        'chmod', '+x', '/tmp/slot-scrapper/slot-scrapper'
    ]
    commands.append((' '.join(chmod_command), "Make binary executable"))

    # execute binary
    run_command = [
        'kubectl', 'exec',
        pod.metadata.name,
        '-n', namespace,
        '--',
        '/tmp/slot-scrapper/slot-scrapper'
    ]
    commands.append((' '.join(run_command), "Execute binary"))

    # delete binary and temp directory
    cleanup_command = [
        'kubectl', 'exec',
        pod.metadata.name,
        '-n', namespace,
        '--',
        'rm', '-rf', '/tmp/slot-scrapper'
    ]
    commands.append((' '.join(cleanup_command), "Cleanup binary and temp directory"))

    if dry_run:
        print(f"\nDry run commands for pod {pod.metadata.name} in namespace {namespace}:")
        for cmd, description in commands:
            print(f"{description}:\n{cmd}\n")
        return None

    # actually execute commands
    for cmd_array, _ in commands:
        if "kubectl cp" in cmd_array:
            print(f"copying binary to pods {pod.metadata.name} in namespace {namespace}")
            # special handling for cp command which needs array form
            subprocess.run(copy_command)
        else:
            # other commands can use the original array form
            cmd_list = cmd_array.split()
            result = subprocess.run(cmd_list, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error executing command: {cmd_array}")
                print(f"Error output: {result.stderr}")
                return None

    # get final result from binary execution
    result = subprocess.run(run_command, capture_output=True, text=True)

    # cleanup temp directory
    subprocess.run(cleanup_command)

    return result.stdout.strip() if result.stdout else None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Only show commands that would be executed')
    args = parser.parse_args()

    # get binary path
    binary_path = os.path.join(os.path.dirname(__file__), 'target', 'release', 'slot-scrapper')

    if args.dry_run:
        print("=== DRY RUN MODE ===")
        namespace_pods = get_ext_namespaces_and_pods()
        for namespace, pods in namespace_pods:
            print(f"\nNamespace: {namespace}")
            for pod in pods:
                copy_and_execute_binary(pod, namespace, binary_path, dry_run=True)
        return

    # get all ext- namespaces and their pods
    namespace_pods = get_ext_namespaces_and_pods()

    total_txs = 0
    results = {}

    # execute binary for each pod in each namespace
    for namespace, pods in namespace_pods:
        namespace_results = []
        print(f"Processing namespace: {namespace}")

        for pod in pods:
            print(f"Processing pod: {pod.metadata.name}")
            result = copy_and_execute_binary(pod, namespace, binary_path, dry_run=False)
            if result:
                try:
                    print(f"result {result}")
                    tx_count = int(result)
                    total_txs += tx_count
                    namespace_results.append({
                        "pod": pod.metadata.name,
                        "transactions": tx_count
                    })
                except ValueError:
                    print(f"Invalid result from pod {pod.metadata.name}: {result}")

        results[namespace] = namespace_results

    # output results
    print("\nResults by namespace:")
    print(json.dumps(results, indent=2))
    print(f"\nTotal transactions across all pods: {total_txs}")

if __name__ == "__main__":
    main()
