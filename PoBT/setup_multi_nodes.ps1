# Step 1: Build the Docker Image
docker build -t pobt-node .

# Step 2: Create a Docker Network
docker network create pobt-network

# Step 3: Create 10 Docker Containers but don't start them

$totalNodes = 50

for ($numNodes = 25; $numNodes -le $totalNodes; $numNodes+=5) {

    Write-Host "Creating $numNodes nodes..."

    for ($j = 1; $j -le $numNodes; $j++) {
        $port = 5000 + $j
        docker create --name "node$j" --network pobt-network -p "${port}:${port}" pobt-node $j "$port" Nodes_map.json $numNodes
    }

    Write-Host "Starting $numNodes nodes..."
    
    # Start the required number of nodes
    for ($j = 1; $j -le $numNodes; $j++) {
        docker start "node$j"
    }

    # Wait for 2 minutes
    Write-Host "Waiting for 2 minutes..."
    Start-Sleep -Seconds 120

    # Create a folder for logs
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    # $logFolder = "Logs_Run_${numNodes}_Nodes_$timestamp"
    $logFolder = "Logs_Run_${numNodes}_Nodes"

    New-Item -ItemType Directory -Path $logFolder

    # Copy logs from containers to the log folder
    for ($j = 1; $j -le $numNodes; $j++) {
        docker cp "node${j}:/app/node_${j}_messages.log" "$logFolder/node_${j}_messages.log"
        docker cp "node${j}:/app/node_${j}_evaluation.log" "$logFolder/node_${j}_evaluation.log"
        docker logs "node$j" > "$logFolder/node_${j}_docker.log"
    }

    # Remove logs and reset containers
    Write-Host "Cleaning up logs in containers..."
    for ($j = 1; $j -le $numNodes; $j++) {
        docker exec "node$j" bash -c "rm -f /app/node_${j}_messages.log /app/node_${j}_evaluation.log"
        docker exec "node$j" bash -c "rm -f /app/node_${j}_messages.log /app/node_${j}_messages.log"
    }

    # Stop all nodes
    '''
    Write-Host "Stopping all nodes..."
    for ($j = 1; $j -le $numNodes; $j++) {
        docker stop "node$j"
    }
    '''

    Write-Host "Removing all nodes..."
    for ($j = 1; $j -le $numNodes; $j++) {
        docker rm -f "node$j"
    }

    
    Write-Host "Completed iteration for $numNodes nodes. Logs saved to $logFolder."
}

docker network rm pobt-network

Write-Host "All iterations completed."
