---
title: Apache Mesos - Beginner Contribution Guide
layout: documentation
---

# Beginner Contributor Guide

## Introduction

The purpose of this document is to provide a first-time introduction to the process of contributing to Mesos. It focuses on submitting small documentation-only patches or trivial fixes via GitHub pull requests. If you'd like an introduction to the standard day-to-day workflow for advanced Mesos contributors, see our [advanced contribution guide](advanced-contribution.md).

## TL;DR
To propose a small change to Mesos, simply open a PR against our public GitHub mirror at [https://github.com/apache/mesos](https://github.com/apache/mesos). Further instructions can be found below if needed.

## Download and Build Mesos

First, download and build the latest development version of the Mesos distribution. In order to submit changes via GitHub pull requests, you need to fork the [Apache Mesos GitHub mirror](https://github.com/apache/mesos). Once you have your own fork, clone it to your local machine using Git.

Once you have the Mesos source code on your local machine, you can install the necessary dependencies and build it. Instructions for this process can be found in the [Getting Started](getting-started.md) page. Note that the `bootstrap` script in the repository's root directory will install git hooks which will help you adhere to Mesos style when committing.

## Find a Problem to Solve

If you already have a specific issue in mind which you want to address in the codebase, that's great! Reach out to some of the [committers](committers.md) on [Mesos Slack](/community) or on the developer mailing list to discuss the improvement you'd like to make. If you want to contribute but aren't sure what to work on, you can find open issues in JIRA, our issue tracker. Asking on Mesos Slack or on the developer mailing list is a great way to find out what you might work on that could have a high impact on the community.

From past experience, we have found that it's best if contributors talk to committers about their fix before writing code. This ensures that the contributor has the context necessary to make the change in a way consistent with the rest of the codebase, and helps avoid time spent on solutions which need major edits in order to get merged. Please chat with us before writing code, we'll help you design a solution!

This GitHub workflow is most appropriate for documentation-only changes and small, trivial fixes. For more significant changes, please use the [advanced contribution workflow](advanced-contribution.md).

## Make Modifications

Once you know what you're going to change, go ahead and make your intended modifications to the codebase and then commit the changes to your local build repository. When making changes, also consider the following:

* Does documentation need to be updated to accommodate this change? (see the `docs/` folder in the repository)
* Do new tests need to be added? (see the `src/tests/` folder in the repository)
* Note that changes to the bundled libraries `3rdparty/libprocess/` and `3rdparty/stout/` must be kept in separate commits from changes to the rest of the codebase

## Open a PR

Once changes are completed and tested, it's time to open a PR so that they can be reviewed. Before pushing your changes to GitHub, organize the commits on your local branch. Make sure that each commit has a commit message which concisely explains the changes being made. Split your changes apart into separate commits, with similar changes collected together. This will help reviewers review your code more quickly and easily.

When your local branch is clean and ready to submit, push it to your Mesos fork. You can then open a PR against the Apache Mesos GitHub repository. Once your PR is open, you can notify the community on Mesos Slack or on the developer mailing list.

# Getting Started Guidance

The following links provide additional guidance as you get started contributing to Apache Mesos.

## JIRA

[JIRA](http://issues.apache.org/jira/browse/MESOS) is the issue tracking system for all bug fixes and feature additions in Mesos. When contributing to Mesos, all assignments, discussions, and resolution proposals for a specific issue should be documented as comments within the JIRA item. If you contribute something which is more than a trivial fix, it should probably be tracked in JIRA.

## Identifying a JIRA Issue

If you'd like to find an existing issue to work on yourself, identify a Mesos JIRA ticket that is currently unassigned. It is highly recommended to start contributing to beginner-level issues and move to more advanced issues over time. The JIRA issue-tracking system can be filtered based on labels. The following labels can be used to identify beginner-level JIRA tickets:

* newbie
* newbie++
* beginner
* beginners

If you'd like to work on existing issues in Mesos, this will likely require the submission of patches large enough that you should use the [advanced contribution workflow](advanced-contribution.md).

## Assign a JIRA Issue

There are a couple options to assign a work item: 1) create a new JIRA issue to work on or 2) identify an existing JIRA issue item to work on as described above. Whichever option is chosen there are several steps that should be followed when assigning yourself a JIRA ticket.

It is important to identify a shepherd before you assign a ticket to yourself. Working with a shepherd will reveal its priority at the current time. To identify a shepherd look at the [maintainers](committers.md) file to get an idea who to ask to shepherd your JIRA issue. You can reach out to them on Mesos Slack, on the developer mailing list, or directly via email.

## JIRA Issue Solution Proposals

Once you have an assigned JIRA issue and you have identified a shepherd, it is important to discuss your proposed solution within the JIRA ticket early in the resolution process in order to get feedback from reviewers. Early discussions will help:

1. ensure the solution will be scoped in a consumable fashion;

2. eliminate duplicate work with other contributions; and

3. alert anyone interested in following the activity and progress of the ticket.


Guidelines to consider when designing a solution can be found in the [effective-code-reviewing](effective-code-reviewing.md) document.

## Making Changes

After completing the solution review make the source code changes, build and successfully run relevant test cases. Guidelines for these processes can be found in the [Advanced Contribution Guide](advanced-contribution.md) and [Mesos C++ Style Guide](c++-style-guide.md).

When creating and running test cases pay particular attention to race conditions. One recommendation is to run system tests multiple times.  Below is a sample command to repeat the test 100 times.


    sudo GLOG_v=1 ./bin/mesos-tests.sh --verbose --gtest_filter="*DOCKER*" --break-on-error --gtest_shuffle --gtest_repeat=100

# Advanced JIRA Tickets

As you gain experience contributing to Mesos you may want to tackle more advanced JIRA tickets. These items may touch multiple components within Mesos and/or may have a significant impact on the developer or user community. In these cases, a working group of stakeholders is formed to develop a design document. The initial formation of this working group will be part of the community communication resources, e.g. the re-occurring developer sync meetings, the developer email list, the IRC channel, etc. For reference, a contributor new to an advanced level work item can refer to the work done for the [inverse offer](https://issues.apache.org/jira/browse/MESOS-1592) project.

When you begin making more complex contributions to Mesos, it will be beneficial to follow the standard code contribution workflow, which makes use of ReviewBoard instead of GitHub. You can find an introduction to this workflow in our [Advanced Contribution Guide](advanced-contribution.md).

-----

# FAQs

Q: Where can I find documentation about Mesos?
A: Mesos documentation is located in the 'docs' directory of the Mesos distribution. This documentation includes information about Mesos architecture and design, running Mesos, developing within the Mesos ecosystem and contributing to Mesos. Additionally, the latest documentation can be found here:
[http://mesos.apache.org/documentation/latest/index.html](http://mesos.apache.org/documentation/latest/index.html)


Q: What is a Shepherd?
A: An identified PMC/committer that works with a contributor to help shepherd a JIRA item to completion.  Shepherds should be as identified at the beginning of the design/development phase.
