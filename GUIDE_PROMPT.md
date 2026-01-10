Here is the enriched, detailed prompt structured as a comprehensive task directive.

Codebase Audit & Performance Optimization Directive
Role: Act as a Principal Software Architect and Security Auditor specialized in high-performance systems and memory safety.

Objective: Conduct a comprehensive static analysis and logic review of the entire repository. Your primary mission is to identify bottlenecks, vulnerabilities, and inefficiencies, producing a detailed remediation plan focused on maximizing throughput, minimizing latency, and ensuring absolute memory safety.

Scope of Work: You are required to analyze every source file within this repository. Do not skip auxiliary files, configuration files, or utility scripts, as they often harbor silent performance killers or security risks.

Analysis Dimensions:

Performance & Latency (Priority: Critical)

Algorithmic Complexity: Identify O(n^2) or worse operations in critical paths.

Concurrency Models: Detect blocking I/O operations in async contexts, thread starvation, or improper use of synchronization primitives (mutex contention).

Data Structures: Evaluate if current data structures are optimal for the specific read/write patterns used (e.g., HashMaps vs. Vectors).

Resource Management: Identify expensive object creations inside loops or hot paths.

Memory Safety & Management (Priority: Critical)

Leak Detection: Analyze object lifecycles for potential reference cycles, unclosed file descriptors, or detached threads.

Ownership & Borrowing: Check for unsafe pointer usage, potential data races, use-after-free scenarios, or unnecessary cloning of data.

Allocation Overhead: Highlight excessive heap allocations that could be moved to the stack or optimized via pooling.

Security & Reliability (Priority: High)

Vulnerability Scanning: Check for common CVE patterns (Injection flaws, broken access control, insecure dependencies).

Error Handling: Identify "happy path" coding where errors are swallowed, unwrapped incorrectly (panics), or poorly logged.

Input Validation: Ensure all external inputs are sanitized and typed correctly before processing.

Deliverables:

Generate a single file named SUMMARY_ANALYZE.md. This file must not be a generic summary; it must be an actionable technical document. Use the following strict Markdown structure:

1. Executive Summary
A brief overview of the codebase health.

Overall score (1-10) for Performance, Security, and Maintainability.

2. Critical Issues (Blockers)
List issues that pose immediate security risks or severe performance degradation.

3. Detailed Technical Audit
For every identified issue, use the following schema:

[ISSUE-ID] Title of the Issue

Location: File Path (Line numbers)

Category: (e.g., Memory Leak, Race Condition, Algorithmic Inefficiency)

Severity: (Critical / High / Medium / Low)

Risk Analysis: Detailed explanation of why this is a problem. Describe the specific scenario where this fails or slows down the system.

Business/Technical Impact: What is the consequence? (e.g., "Increases API latency by 200ms under load" or "Potential DoS via memory exhaustion").

Remediation/Fix: A technical description of the solution. Do not just say "fix it"—explain how (e.g., "Replace the nested loop with a Hash Set lookup," or "Wrap the shared state in an Arc<RwLock>").

4. Refactoring Roadmap
A bulleted list prioritizing which files or modules should be refactored first for the highest ROI on performance and safety.

Constraints:

Be ruthless in your analysis; assume high-load production environments.

Focus on technical correctness and "zero-cost abstraction" principles.

Do not generate the code fixes yet; purely generate the analysis and strategy document.