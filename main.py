import argparse
import sys
import os

# Add src to path to allow importing spend package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from mailtx import ingest, parser, embed, ledger, query_engine, db

def main():
    # Ensure database is initialized
    db.init_db()

    parser_arg = argparse.ArgumentParser(description="Local-first AI Spend Analyzer")
    subparsers = parser_arg.add_subparsers(dest="command", help="Available commands")


    ingest_parser = subparsers.add_parser("ingest", help="Download and parse emails")
    ingest_parser.add_argument("--days", type=int, default=90, help="Number of days to look back (default: 90)")

    subparsers.add_parser("embed", help="Generate embeddings for emails")


    subparsers.add_parser("extract", help="Extract transactions from emails")

    ask_parser = subparsers.add_parser("ask", help="Ask a natural language question")
    ask_parser.add_argument("query", type=str, help="The question to ask (e.g., 'How much spent on Uber?')")

    args = parser_arg.parse_args()

    if args.command == "ingest":
        print(f"Starting ingestion for last {args.days} days...")
        ingest.download_recent_emails(days=args.days)
        print("\nStarting parsing...")
        parser.process_raw_files()
        
    elif args.command == "embed":
        print("Generating embeddings...")
        embed.generate_embeddings()
        
    elif args.command == "extract":
        print("Building ledger (extracting transactions)...")
        ledger.build_ledger()
        
    elif args.command == "ask":
        print(f"Analyzing query: '{args.query}'...")
        params = query_engine.parse_intent(args.query)
        if params:
            results = query_engine.execute_query(params)
            print("\n" + query_engine.format_result(results, params))
        else:
            print("Could not understand the query.")
            
    else:
        parser_arg.print_help()

if __name__ == "__main__":
    main()
