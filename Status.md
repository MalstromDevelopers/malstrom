# What is this?

This document serves to reming me where I left off when I last worked on this project

# 2026-01-04

I do not understand where I left off, I am currently working on re-creating the code for the distributed
and ICA stuff, but currently only mocking out the pub/pub(crate) APIs

Below is the general architecture I apparently came up with last time with three operators

input_recv
 - in:
    - local messages
    - remote messages
- out:
    versioned messages with sender
- tasks:
    - keep client set up to date
    - align barrier
 
state-handler:
    - in:
        versioned messages with sender
    - out:
        versioned message with sender
    - tasks:
        - run ICA algorithm
        - buffer collected messages
        
output_send:
    - in:
        versioned message with sender
    - out:
        - Wiremessage (remote)
        - normal message (local)
    - tasks:
        - route messages
        - use correct router in ICA process