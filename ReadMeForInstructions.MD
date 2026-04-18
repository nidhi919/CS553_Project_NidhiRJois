# CS553_2026 - Distributed Algorithms Framework with NetGameSim Integration

## Prerequisites
- Java 11 or higher
- IntelliJ IDEA (with Scala plugin)
- sbt 1.9.x (bundled with IntelliJ)
- NetGameSim repository (for graph generation)

## Note for Graders
- `graph.dot` is NOT included (must be generated via NetGameSim)
- All code compiles and runs on a clean machine
- 15 tests pass with `sbt test`

## Using the sbt Shell in IntelliJ
1. Open the sbt shell: **View → Tool Windows → sbt**
2. The sbt shell will open at the bottom of IntelliJ
3. Type commands directly in the sbt shell

## Building the Project
In the sbt shell, type:
```bash
compile
```

## Running Tests
```bash
test
```

## Step 1: Generate a Graph using NetGameSim

1. Clone NetGameSim separately:
```bash
git clone https://github.com/0x1DOCD00D/NetGameSim.git
cd NetGameSim
sbt run
Copy the generated .dot file to the project root of this repository.

## Step 2: Run Simulations
Simple Node (Background Traffic with Edge Label Enforcement)
In the sbt shell, type:
```bash
runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 30s simple
```

Paxos Consensus Algorithm.
In the sbt shell, type:
```bash
runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 60s paxos
```

Peterson-Kearns Rollback Recovery Algorithm.
In the sbt shell, type:
```bash
runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 60s pk
```
## External Input Injection

### Create the inputs folder and CSV file
1. In IntelliJ, right-click on the project root `CS553_2026`
2. Select **New → Directory** → Name: `inputs`
3. Right-click on `inputs` → **New → File** → Name: `sample_inputs.csv`
4. Add this content:
```csv
# Format: nodeId,messageType,payload,delayMs
5,PING,Hello from file,2000
10,WORK,Process this,5000
20,GOSSIP,Broadcast news,8000
100,CONTROL,Start algorithm,3000
0,PING,Message to timer node,1500
```
Run with file input:
```bash
runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 30s simple inputs/sample_inputs.csv
```


## For Quick Reference!
| Action | sbt shell command |
|--------|-------------------|
| Compile | `compile` |
| Run tests | `test` |
| Run SimpleNode | `runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 30s simple` |
| Run Paxos | `runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 60s paxos` |
| Run Peterson-Kearns | `runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 60s pk` |
| Run with input file | `runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 30s simple inputs/sample_inputs.csv` |
| Interactive mode | `runMain com.uic.cs553.netgamesim.NetGameSimRunner graph.dot 30s simple --interactive` |
