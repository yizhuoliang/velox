#!/usr/bin/env python3

import re
import sys
import os
import matplotlib
# Use the Agg backend for environments without a display
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import random

def parse_log_file(filename):
    """
    Parse the log file and extract the task data for both rounds.
    Returns tuple of (round1_triple, round1_sin, round2_triple, round2_sin)
    """
    with open(filename, 'r') as f:
        content = f.read()
    
    # Split into rounds - more robust approach
    if '==================== Round 1 ====================' in content and '==================== Round 2 ====================' in content:
        # Split by the headers to get the content for each round
        parts = content.split('==================== Round 1 ====================', 1)
        if len(parts) > 1:
            round1_part = parts[1].split('==================== Round 2 ====================', 1)[0]
            round2_part = parts[1].split('==================== Round 2 ====================', 1)[1] if len(parts[1].split('==================== Round 2 ====================', 1)) > 1 else ""
        else:
            round1_part = ""
            round2_part = ""
    else:
        # Fallback if the exact headers aren't found
        sections = content.split('====================')
        round1_part = sections[1] if len(sections) > 1 else ""
        round2_part = sections[2] if len(sections) > 2 else ""
    
    # Parse round 1 data
    round1_triple = parse_tasks(round1_part, "triple")
    round1_sin = parse_tasks(round1_part, "sin")
    
    # Parse round 2 data
    round2_triple = parse_tasks(round2_part, "triple")
    round2_sin = parse_tasks(round2_part, "sin")
    
    print(f"Parsed {len(round1_triple)} triple tasks and {len(round1_sin)} sin tasks for Round 1")
    print(f"Parsed {len(round2_triple)} triple tasks and {len(round2_sin)} sin tasks for Round 2")
    
    return round1_triple, round1_sin, round2_triple, round2_sin

def parse_tasks(round_data, task_type):
    """
    Parse tasks of a specific type (triple or sin) from a round's data.
    Returns a list of task dictionaries.
    """
    tasks = []
    
    # Regular expression to match task lines
    # Note: The 'ms' at the end is optional since it might be excluded in some logs
    pattern = r"^\s*" + re.escape(task_type) + r"\[(\d+)\]\s+start=(\d+)\s+end=(\d+)\s+dur=(\d+)(?:\s+ms)?"
    
    # Find all matches in the round data
    for line in round_data.split('\n'):
        match = re.search(pattern, line)
        if match:
            index, start, end, duration = match.groups()
            tasks.append({
                "task": f"{task_type}[{index}]",
                "type": task_type,
                "index": int(index),
                "start": int(start),
                "end": int(end),
                "duration": int(duration)
            })
    
    return tasks

def compute_metrics(triple_tasks, sin_tasks):
    """Compute and return metrics for the tasks."""
    metrics = {}
    
    # Average durations
    metrics['avg_triple_duration'] = compute_average_duration(triple_tasks)
    metrics['avg_sin_duration'] = compute_average_duration(sin_tasks)
    
    # Combine all tasks
    all_tasks = triple_tasks + sin_tasks
    
    if not all_tasks:
        metrics['makespan'] = 0
        metrics['min_start'] = 0
        metrics['max_end'] = 0
        return metrics
    
    # Makespan (total execution time from first start to last end)
    min_start = min(t['start'] for t in all_tasks)
    max_end = max(t['end'] for t in all_tasks)
    makespan = max_end - min_start
    
    metrics['makespan'] = makespan
    metrics['min_start'] = min_start
    metrics['max_end'] = max_end
    
    return metrics

def compute_average_duration(tasks):
    """Return the average 'duration' across a list of tasks (0 if empty)."""
    if not tasks:
        return 0.0
    return sum(t["duration"] for t in tasks) / len(tasks)

def plot_round_timeline(ax, triple_tasks, sin_tasks, round_title, x_max_range):
    """
    Plots a timeline of triple vs. sin tasks on ax, with tasks sorted by start time.
    x_max_range is the shared maximum time range among all rounds,
    so we set x-limit to [0, x_max_range + 50].
    """
    # Combine tasks to find earliest start in this round
    all_tasks = triple_tasks + sin_tasks
    if not all_tasks:
        return
        
    min_start = min(t['start'] for t in all_tasks)
    
    # Sort tasks by start time, but randomize order for tasks with the same start time
    # First, we'll group tasks by start time
    tasks_by_start_time = {}
    for task in all_tasks:
        start_time = task['start']
        if start_time not in tasks_by_start_time:
            tasks_by_start_time[start_time] = []
        tasks_by_start_time[start_time].append(task)
    
    # Now create a sorted list with randomized order for same-time tasks
    sorted_tasks = []
    for start_time in sorted(tasks_by_start_time.keys()):
        # Shuffle tasks with the same start time
        same_time_tasks = tasks_by_start_time[start_time]
        random.shuffle(same_time_tasks)
        sorted_tasks.extend(same_time_tasks)
    
    # We'll place tasks in ascending Y order, then invert the axis
    y = 0
    
    # Plot each task based on its type
    for task in sorted_tasks:
        left = task["start"] - min_start
        width = task["duration"]
        
        # Choose color based on task type
        color = 'tab:blue' if task["type"] == "triple" else 'tab:orange'
        
        rect = mpatches.Rectangle((left, y), width, 0.8, color=color, alpha=0.6)
        ax.add_patch(rect)
        
        y += 1

    total_bars = len(all_tasks)

    # Shared X range
    ax.set_xlim(0, x_max_range + 50)
    ax.set_ylim(-0.5, total_bars - 0.5)
    ax.invert_yaxis()  # so earliest starting task is at the top

    # Labels
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("Tasks (sorted by start time)")
    
    # Compute metrics for this round
    metrics = compute_metrics(triple_tasks, sin_tasks)
    
    # Set title with extra padding to place it above the legend
    ax.set_title(round_title, pad=70)

    # Legend text with average durations
    avg_triple = metrics['avg_triple_duration']
    avg_sin = metrics['avg_sin_duration']
    triple_label = f"Projection: A * B + C (avg {avg_triple:.1f} ms)"
    sin_label = f"Projection: sin(A) (avg {avg_sin:.1f} ms)"

    triple_patch = mpatches.Patch(color='tab:blue', alpha=0.6, label=triple_label)
    sin_patch = mpatches.Patch(color='tab:orange', alpha=0.6, label=sin_label)
    
    # Place legend below the title but above the plot
    ax.legend(handles=[triple_patch, sin_patch], loc='lower center', bbox_to_anchor=(0.5, 1.0), ncol=2)
    
    return metrics

def main():
    # Check if a filename was provided
    if len(sys.argv) < 2:
        print("Usage: python log_parser.py <log_file> [output_image_file]")
        sys.exit(1)
        
    # Parse the log file
    filename = sys.argv[1]
    try:
        round1_triple, round1_sin, round2_triple, round2_sin = parse_log_file(filename)
    except Exception as e:
        print(f"Error parsing log file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Check if we parsed any data
    if not round1_triple and not round1_sin and not round2_triple and not round2_sin:
        print("Warning: No task data was found in the log file.")
        sys.exit(1)
    
    # Even bigger font sizes
    plt.rcParams.update({
        'font.size': 30,
        'axes.labelsize': 30,
        'axes.titlesize': 36,
        'xtick.labelsize': 28,
        'ytick.labelsize': 28,
        'legend.fontsize': 28,
        'figure.titlesize': 42,
    })

    # Set a random seed for reproducibility
    random.seed(42)
    
    # Compute metrics for both rounds to determine shared x-axis range
    metrics_r1 = compute_metrics(round1_triple, round1_sin)
    metrics_r2 = compute_metrics(round2_triple, round2_sin)
    
    # Shared X-axis range for both subplots
    x_max_range = max(metrics_r1['makespan'], metrics_r2['makespan'])

    # Create figure with wider aspect ratio
    fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(22, 18))
    
    # Add space between subplots
    plt.subplots_adjust(hspace=0.4)

    # Plot Round 1 => "High contention"
    metrics_r1 = plot_round_timeline(ax1, round1_triple, round1_sin, "High contention", x_max_range)

    # Plot Round 2 => "Low contention"
    metrics_r2 = plot_round_timeline(ax2, round2_triple, round2_sin, "Low contention", x_max_range)

    # Print detailed metrics to console
    print("\n----- Performance Metrics -----")
    print("Round 1 (High contention):")
    print(f"  Triple tasks: {len(round1_triple)} tasks, avg duration: {metrics_r1['avg_triple_duration']:.2f} ms")
    print(f"  Sin tasks: {len(round1_sin)} tasks, avg duration: {metrics_r1['avg_sin_duration']:.2f} ms")
    print(f"  Makespan: {metrics_r1['makespan']} ms (from {metrics_r1['min_start']} to {metrics_r1['max_end']})")
    
    print("\nRound 2 (Low contention):")
    print(f"  Triple tasks: {len(round2_triple)} tasks, avg duration: {metrics_r2['avg_triple_duration']:.2f} ms")
    print(f"  Sin tasks: {len(round2_sin)} tasks, avg duration: {metrics_r2['avg_sin_duration']:.2f} ms")
    print(f"  Makespan: {metrics_r2['makespan']} ms (from {metrics_r2['min_start']} to {metrics_r2['max_end']})")
    
    # Improvement percentages
    print("\nImprovement metrics:")
    if metrics_r1['makespan'] > 0:
        makespan_improv = (1 - metrics_r2['makespan'] / metrics_r1['makespan']) * 100
        print(f"  Makespan improvement: {makespan_improv:.2f}%")
    
    if metrics_r1['avg_triple_duration'] > 0:
        triple_time_improv = (1 - metrics_r2['avg_triple_duration'] / metrics_r1['avg_triple_duration']) * 100
        print(f"  Triple task average duration improvement: {triple_time_improv:.2f}%")
    
    if metrics_r1['avg_sin_duration'] > 0:
        sin_time_improv = (1 - metrics_r2['avg_sin_duration'] / metrics_r1['avg_sin_duration']) * 100
        print(f"  Sin task average duration improvement: {sin_time_improv:.2f}%")

    # This tight_layout will respect the title padding we set
    plt.tight_layout()
    
    # Determine output filename (use command line arg if provided, otherwise derive from input file)
    if len(sys.argv) > 2:
        output_filename = sys.argv[2]
    else:
        output_filename = f"{os.path.splitext(filename)[0]}_visualization.png"
    
    print(f"\nSaving visualization to {output_filename}")
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')

if __name__ == "__main__":
    main()