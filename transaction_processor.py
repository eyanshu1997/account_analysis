#!/usr/bin/env python3
"""
Account Statement Processor

A Python program to process Excel account statements.
This will be built step by step, starting with basic file reading functionality.
"""

import pandas as pd
import os
import json
from pathlib import Path
from datetime import datetime


class AccountStatementProcessor:
    """Main class to handle account statement processing."""
    
    def __init__(self):
        self.data = None
        self.expenses_df = None
        self.income_df = None
        self.savings_df = None
        self.file_path = None
        self.config_dir = Path(__file__).parent / "config"
        self.config_dir.mkdir(exist_ok=True)
        
        # Working directories
        self.working_dir = Path(__file__).parent / "working"
        self.current_dir = self.working_dir / "current"
        self.saves_dir = Path(__file__).parent / "saves"
        
        # Create directories
        self.working_dir.mkdir(exist_ok=True)
        self.current_dir.mkdir(exist_ok=True)
        self.saves_dir.mkdir(exist_ok=True)
        
        # Active config lives in current working directory
        self.categories_file = self.current_dir / "categories.json"
        self.rules_file = self.current_dir / "rules.json"

        # Seed active config from defaults if missing
        default_categories = self.config_dir / "default_categories.json"
        default_rules = self.config_dir / "default_rules.json"
        if not self.categories_file.exists() and default_categories.exists():
            self.categories_file.write_text(default_categories.read_text())
        if not self.rules_file.exists() and default_rules.exists():
            self.rules_file.write_text(default_rules.read_text())
        self._load_categories()
        self._load_rules()
        
        # Try to load current working state on initialization
        self._load_current_state()
    
    def load_file(self, file_path: str):
        """
        Load account statement from Excel file.
        
        Args:
            file_path (str): Path to the Excel account statement file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.file_path = Path(file_path)
        file_extension = self.file_path.suffix.lower()
        
        try:
            if file_extension in ['.xls', '.xlsx']:
                # Read the raw Excel file to find the actual data headers
                raw_data = pd.read_excel(file_path, header=None)
                
                # Find the row with the actual column headers
                header_row = self._find_header_row(raw_data)
                
                if header_row is not None:
                    # Skip to actual data rows (usually 1-2 rows after header)
                    data_start_row = header_row + 1
                    
                    # Skip any rows with asterisks or formatting
                    while data_start_row < len(raw_data):
                        test_row = raw_data.iloc[data_start_row]
                        test_values = [str(val).strip() for val in test_row.values if pd.notna(val)]
                        if test_values and not test_values[0].startswith('*'):
                            break
                        data_start_row += 1
                    
                    # Read the data starting from the actual transaction rows
                    self.data = pd.read_excel(file_path, skiprows=data_start_row, header=None)
                    
                    # Set the correct column names
                    expected_columns = ['Date', 'Narration', 'Chq./Ref.No.', 'Value Dt', 'Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
                    
                    # Only use as many columns as we have data for
                    num_cols = min(len(expected_columns), len(self.data.columns))
                    self.data.columns = expected_columns[:num_cols] + [f'Unnamed_{i}' for i in range(num_cols, len(self.data.columns))]
                    
                    # Clean up any empty rows and columns
                    self.data = self._clean_data()
                    
                else:
                    # Fallback to original method if header not found
                    self.data = raw_data
                
                # Add category column if it doesn't exist
                if 'Category' not in self.data.columns:
                    self.data['Category'] = 'Uncategorized'
                
                # Split data into expenses and income
                self._split_expense_income()
                
                # Save to current working state
                self.save_current_state()
                
            else:
                raise ValueError(f"Unsupported file format: {file_extension}. Only .xls and .xlsx files are supported.")
            
        except Exception as e:
            raise
    

    
    def _find_header_row(self, raw_data):
        """Find the row containing the transaction headers."""
        expected_headers = ['Date', 'Narration', 'Chq./Ref.No.', 'Value Dt', 'Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
        
        for i, row in raw_data.iterrows():
            row_values = [str(val).strip() for val in row.values if pd.notna(val)]
            row_text = ' '.join(row_values).lower()
            
            # Check if this row contains the exact pattern we're looking for
            if 'date' in row_text and 'narration' in row_text and ('withdrawal' in row_text or 'deposit' in row_text):
                # Check if the next few rows actually contain data (not just asterisks)
                data_start = i + 1
                while data_start < len(raw_data) and data_start < i + 5:
                    next_row = raw_data.iloc[data_start]
                    next_values = [str(val).strip() for val in next_row.values if pd.notna(val)]
                    # Look for a row that doesn't start with asterisks (actual data)
                    if next_values and not next_values[0].startswith('*'):
                        return i
                    data_start += 1
                
                return i
        
        return None
    
    def _clean_data(self):
        """Clean the transaction data by removing empty rows and standardizing columns."""
        if self.data is None:
            return None
        
        # Remove rows where all values are NaN
        cleaned_data = self.data.dropna(how='all')
        
        # Remove rows where Date column is NaN (likely not transaction rows)
        if 'Date' in cleaned_data.columns:
            cleaned_data = cleaned_data.dropna(subset=['Date'])
        
        # Additional validation: remove rows with invalid date formats or amounts
        cleaned_data = self._validate_transaction_data(cleaned_data)
        
        # Reset index after cleaning
        cleaned_data = cleaned_data.reset_index(drop=True)
        
        
        return cleaned_data
    
    def _validate_transaction_data(self, data):
        """Validate and clean transaction data using ID-based approach."""
        if data is None or len(data) == 0:
            return data
        
        # Create unique ID column based on Chq./Ref.No. or generate random ID
        import uuid
        
        def create_transaction_id(row):
            if pd.notna(row['Chq./Ref.No.']) and str(row['Chq./Ref.No.']).strip():
                return str(row['Chq./Ref.No.']).strip()
            else:
                return f"TXN_{str(uuid.uuid4())[:8]}"
        
        data['Transaction_ID'] = data.apply(create_transaction_id, axis=1)
        
        # Add Notes/Sub Category column
        data['Notes'] = ''
        
        # Remove specific transaction based on narration
        rajni_transfer_mask = data['Narration'].str.contains('IMPS-431214353598-RAJNI KHANNA-JAKA-XXXXXXXXXXXX2675-ASHIMA', na=False)
        if rajni_transfer_mask.any():
            removed_count = rajni_transfer_mask.sum()
            data = data[~rajni_transfer_mask]

        
        # Remove transactions with invalid dates (should look like DD/MM/YY format)
        if 'Date' in data.columns:
            # Keep only rows where Date looks like a proper date format
            valid_date_mask = data['Date'].astype(str).str.match(r'^\d{2}/\d{2}/\d{2,4}$', na=False)
            invalid_dates = (~valid_date_mask).sum()

            data = data[valid_date_mask]
        
        return data
    
    def _split_expense_income(self):
        """Split transactions into separate DataFrames for expenses, income, and savings."""
        if self.data is None:
            return
        
        # Convert amount columns to numeric
        if 'Withdrawal Amt.' in self.data.columns:
            self.data['Withdrawal Amt.'] = pd.to_numeric(self.data['Withdrawal Amt.'], errors='coerce')
        if 'Deposit Amt.' in self.data.columns:
            self.data['Deposit Amt.'] = pd.to_numeric(self.data['Deposit Amt.'], errors='coerce')
        
        # Create expenses DataFrame (withdrawals excluding savings)
        if 'Withdrawal Amt.' in self.data.columns:
            withdrawal_mask = self.data['Withdrawal Amt.'].notna() & (self.data['Withdrawal Amt.'] > 0)
            # Exclude savings from expenses
            savings_mask = self.data['Category'].isin(['Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP'])
            
            self.expenses_df = self.data[withdrawal_mask & ~savings_mask].copy()
            self.expenses_df = self.expenses_df.reset_index(drop=True)

            # Create savings DataFrame (withdrawals categorized as savings)
            self.savings_df = self.data[withdrawal_mask & savings_mask].copy()
            self.savings_df = self.savings_df.reset_index(drop=True)
        
        # Create income DataFrame (deposits)
        if 'Deposit Amt.' in self.data.columns:
            self.income_df = self.data[self.data['Deposit Amt.'].notna() & (self.data['Deposit Amt.'] > 0)].copy()
            self.income_df = self.income_df.reset_index(drop=True)

    
    def categorize_transaction(self, transaction_index, category, df_type='expense'):
        """Categorize a specific transaction by its index."""
        if df_type == 'expense' and self.expenses_df is not None:
            if 0 <= transaction_index < len(self.expenses_df):
                old_category = self.expenses_df.loc[transaction_index, 'Category']
                self.expenses_df.loc[transaction_index, 'Category'] = category
                
                # Check if this should be moved to savings
                if category in ['Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP']:
                    # Move to savings dataframe
                    transaction_row = self.expenses_df.iloc[transaction_index].copy()
                    if self.savings_df is None:
                        self.savings_df = pd.DataFrame()
                    self.savings_df = pd.concat([self.savings_df, transaction_row.to_frame().T], ignore_index=True)
                    # Remove from expenses
                    self.expenses_df = self.expenses_df.drop(self.expenses_df.index[transaction_index]).reset_index(drop=True)
                
                # Also update in main dataframe
                original_idx = self.expenses_df.index[transaction_index] if transaction_index < len(self.expenses_df) else None
                expense_mask = self.data['Withdrawal Amt.'].notna() & (self.data['Withdrawal Amt.'] > 0)
                expense_indices = self.data[expense_mask].index
                if transaction_index < len(expense_indices):
                    main_idx = expense_indices[transaction_index]
                    self.data.loc[main_idx, 'Category'] = category
                return True
        elif df_type == 'income' and self.income_df is not None:
            if 0 <= transaction_index < len(self.income_df):
                self.income_df.loc[transaction_index, 'Category'] = category
                return True
        elif df_type == 'savings' and self.savings_df is not None:
            if 0 <= transaction_index < len(self.savings_df):
                old_category = self.savings_df.loc[transaction_index, 'Category']
                self.savings_df.loc[transaction_index, 'Category'] = category
                
                # Check if this should be moved back to expenses
                if category not in ['Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP']:
                    # Move to expenses dataframe
                    transaction_row = self.savings_df.iloc[transaction_index].copy()
                    if self.expenses_df is None:
                        self.expenses_df = pd.DataFrame()
                    self.expenses_df = pd.concat([self.expenses_df, transaction_row.to_frame().T], ignore_index=True)
                    # Remove from savings
                    self.savings_df = self.savings_df.drop(self.savings_df.index[transaction_index]).reset_index(drop=True)
                return True
        return False
    
    def remove_transaction(self, transaction_index, df_type='expense'):
        """Remove a transaction by marking it as invalid."""
        if df_type == 'expense' and self.expenses_df is not None:
            if 0 <= transaction_index < len(self.expenses_df):
                # Mark as removed by setting a special category
                self.expenses_df.loc[transaction_index, 'Category'] = 'REMOVED'
                # Remove from expenses_df
                self.expenses_df = self.expenses_df.drop(self.expenses_df.index[transaction_index]).reset_index(drop=True)
                return True
        return False
    
    def categorize_transaction_by_id(self, transaction_id, category, notes=''):
        """Categorize a specific transaction by its ID."""
        updated = False
        
        # Update in main dataframe
        mask = self.data['Transaction_ID'] == transaction_id
        if mask.any():
            self.data.loc[mask, 'Category'] = category
            if notes:
                self.data.loc[mask, 'Notes'] = notes
            updated = True
        
        # Update in expenses dataframe if exists
        if self.expenses_df is not None:
            mask = self.expenses_df['Transaction_ID'] == transaction_id
            if mask.any():
                self.expenses_df.loc[mask, 'Category'] = category
                if notes:
                    self.expenses_df.loc[mask, 'Notes'] = notes
                updated = True
        
        # Update in income dataframe if exists
        if self.income_df is not None:
            mask = self.income_df['Transaction_ID'] == transaction_id
            if mask.any():
                self.income_df.loc[mask, 'Category'] = category
                if notes:
                    self.income_df.loc[mask, 'Notes'] = notes
                updated = True
        
        return updated

    def mark_as_saving(self, transaction_id):
        """Move a transaction from expenses to savings dataframe."""
        # Find the transaction in expenses_df
        if self.expenses_df is not None:
            mask = self.expenses_df['Transaction_ID'] == transaction_id
            if mask.any():
                # Get the transaction
                transaction_row = self.expenses_df[mask].copy()
                transaction_row['Category'] = 'Saving'
                
                # Initialize savings_df if it doesn't exist
                if self.savings_df is None:
                    self.savings_df = pd.DataFrame()
                
                # Add to savings_df
                self.savings_df = pd.concat([self.savings_df, transaction_row], ignore_index=True)
                
                # Remove from expenses_df
                self.expenses_df = self.expenses_df.drop(self.expenses_df[mask].index).reset_index(drop=True)
                
                # Update main dataframe
                main_mask = self.data['Transaction_ID'] == transaction_id
                if main_mask.any():
                    self.data.loc[main_mask, 'Category'] = 'Saving'
                
                return True
        return False

    def move_from_saving_to_expense(self, transaction_id, category='Uncategorized'):
        """Move a transaction from savings back to expenses dataframe."""
        # Find the transaction in savings_df
        if self.savings_df is not None:
            mask = self.savings_df['Transaction_ID'] == transaction_id
            if mask.any():
                # Get the transaction
                transaction_row = self.savings_df[mask].copy()
                transaction_row['Category'] = category
                
                # Add to expenses_df
                if self.expenses_df is None:
                    self.expenses_df = pd.DataFrame()
                
                self.expenses_df = pd.concat([self.expenses_df, transaction_row], ignore_index=True)
                
                # Remove from savings_df
                self.savings_df = self.savings_df.drop(self.savings_df[mask].index).reset_index(drop=True)
                
                # Update main dataframe
                main_mask = self.data['Transaction_ID'] == transaction_id
                if main_mask.any():
                    self.data.loc[main_mask, 'Category'] = category
                
                return True
        return False

    def apply_initial_categorizations(self):
        """Apply basic initial setup - all transactions start as Uncategorized."""
        # All transactions start as 'Uncategorized' by default
        # Users can manually categorize or use rules (when enabled)
        print("✓ Data loaded - all transactions marked as Uncategorized")
        

    
    def save_state(self, output_dir='./exports', use_timestamp=True):
        """Save current state of dataframes to files."""
        # Exports are disabled; return an empty list to indicate no files were written.
        return []
    
    def load_state(self, transactions_file, expenses_file=None, income_file=None, savings_file=None, ignored_file=None):
        """Load dataframes from saved files."""
        try:
            # Load main transactions
            if os.path.exists(transactions_file):
                # Read all as strings to avoid mixed-type DtypeWarning; we'll coerce numerics later
                self.data = pd.read_csv(transactions_file, dtype=str)

            
            # Load expenses if provided
            has_expenses = expenses_file and os.path.exists(expenses_file)
            if has_expenses:
                self.expenses_df = pd.read_csv(expenses_file, dtype=str)

            
            # Load income if provided
            has_income = income_file and os.path.exists(income_file)
            if has_income:
                self.income_df = pd.read_csv(income_file, dtype=str)

            
            # Load savings if provided
            has_savings = savings_file and os.path.exists(savings_file)
            if has_savings:
                self.savings_df = pd.read_csv(savings_file, dtype=str)

            # Load ignored if provided
            has_ignored = ignored_file and os.path.exists(ignored_file)
            if has_ignored:
                self.ignored_df = pd.read_csv(ignored_file, dtype=str)
            
            # Coerce numeric columns if frames are present
            if self.data is not None:
                if 'Withdrawal Amt.' in self.data.columns:
                    self.data['Withdrawal Amt.'] = pd.to_numeric(self.data['Withdrawal Amt.'], errors='coerce')
                if 'Deposit Amt.' in self.data.columns:
                    self.data['Deposit Amt.'] = pd.to_numeric(self.data['Deposit Amt.'], errors='coerce')
            if self.expenses_df is not None and not self.expenses_df.empty:
                if 'Withdrawal Amt.' in self.expenses_df.columns:
                    self.expenses_df['Withdrawal Amt.'] = pd.to_numeric(self.expenses_df['Withdrawal Amt.'], errors='coerce')
            if self.income_df is not None and not self.income_df.empty:
                if 'Deposit Amt.' in self.income_df.columns:
                    self.income_df['Deposit Amt.'] = pd.to_numeric(self.income_df['Deposit Amt.'], errors='coerce')
            if self.savings_df is not None and not self.savings_df.empty:
                if 'Withdrawal Amt.' in self.savings_df.columns:
                    self.savings_df['Withdrawal Amt.'] = pd.to_numeric(self.savings_df['Withdrawal Amt.'], errors='coerce')
            if hasattr(self, 'ignored_df') and self.ignored_df is not None and not self.ignored_df.empty:
                # Treat amounts similar to expenses for ignored (typically withdrawals)
                if 'Withdrawal Amt.' in self.ignored_df.columns:
                    self.ignored_df['Withdrawal Amt.'] = pd.to_numeric(self.ignored_df['Withdrawal Amt.'], errors='coerce')

            # Only recompute split if missing corresponding saved CSVs
            if not has_expenses or not has_income or not has_savings:
                self._split_expense_income()
            
            return True
        except Exception as e:
            return False
    
    def _load_current_state(self):
        """Load current working state if it exists."""
        current_transactions = self.current_dir / "transactions.csv"
        current_expenses = self.current_dir / "expenses.csv"
        current_income = self.current_dir / "income.csv"
        current_savings = self.current_dir / "savings.csv"
        current_ignored = self.current_dir / "ignored.csv"
        
        if current_transactions.exists():
            try:
                print("📂 Loading current working state...")
                self.load_state(
                    str(current_transactions),
                    str(current_expenses) if current_expenses.exists() else None,
                    str(current_income) if current_income.exists() else None,
                    str(current_savings) if current_savings.exists() else None,
                    str(current_ignored) if current_ignored.exists() else None
                )
                print("✓ Current working state loaded")
            except Exception as e:
                print(f"⚠️  Could not load current state: {e}")
    
    def save_current_state(self):
        """Save current state to the working directory."""
        try:
            # Clear current directory first
            for file in self.current_dir.glob("*.csv"):
                file.unlink()
            
            saved_files = []
            
            # Save main dataframe
            if self.data is not None:
                current_file = self.current_dir / "transactions.csv"
                self.data.to_csv(current_file, index=False)
                saved_files.append(str(current_file))
            
            # Save expenses dataframe
            if self.expenses_df is not None and not self.expenses_df.empty:
                expenses_file = self.current_dir / "expenses.csv"
                self.expenses_df.to_csv(expenses_file, index=False)
                saved_files.append(str(expenses_file))
            
            # Save income dataframe
            if self.income_df is not None and not self.income_df.empty:
                income_file = self.current_dir / "income.csv"
                self.income_df.to_csv(income_file, index=False)
                saved_files.append(str(income_file))
            
            # Save savings dataframe
            if hasattr(self, 'savings_df') and self.savings_df is not None and not self.savings_df.empty:
                savings_file = self.current_dir / "savings.csv"
                self.savings_df.to_csv(savings_file, index=False)
                saved_files.append(str(savings_file))

            # Save ignored dataframe
            if hasattr(self, 'ignored_df') and self.ignored_df is not None and not self.ignored_df.empty:
                ignored_file = self.current_dir / "ignored.csv"
                self.ignored_df.to_csv(ignored_file, index=False)
                saved_files.append(str(ignored_file))
            
            return saved_files
        except Exception as e:
            print(f"⚠️  Could not save current state: {e}")
            return []
    
    def save_named_state(self, save_name):
        """Save current state to a named save directory."""
        import shutil
        
        if not save_name or not save_name.strip():
            raise ValueError("Save name cannot be empty")
        
        # Clean the save name
        safe_name = "".join(c for c in save_name if c.isalnum() or c in (' ', '-', '_')).strip()
        if not safe_name:
            raise ValueError("Invalid save name")
        
        save_path = self.saves_dir / safe_name
        
        try:
            # Ensure current state CSVs are up-to-date
            self.save_current_state()

            # Ensure active config exists in current_dir (seed from defaults if missing)
            default_categories = self.config_dir / "default_categories.json"
            default_rules = self.config_dir / "default_rules.json"
            if not self.categories_file.exists() and default_categories.exists():
                self.categories_file.write_text(default_categories.read_text())
            if not self.rules_file.exists() and default_rules.exists():
                self.rules_file.write_text(default_rules.read_text())

            # Remove existing save if it exists
            if save_path.exists():
                shutil.rmtree(save_path)
            
            # Copy current directory to save location
            shutil.copytree(self.current_dir, save_path)
            
            # Also create a metadata file
            metadata = {
                'save_name': save_name,
                'created_at': datetime.now().isoformat(),
                'original_file': str(self.file_path) if self.file_path else None
            }
            
            metadata_file = save_path / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"✓ State saved as '{save_name}'")
            return str(save_path)
        except Exception as e:
            raise e
    
    def load_named_state(self, save_name):
        """Load a named save state to current working directory."""
        import shutil
        
        save_path = self.saves_dir / save_name
        
        if not save_path.exists():
            raise FileNotFoundError(f"Save '{save_name}' not found")
        
        try:
            # Clear current directory
            if self.current_dir.exists():
                shutil.rmtree(self.current_dir)
            
            # Copy save to current directory
            shutil.copytree(save_path, self.current_dir)
            
            # Load the state
            self._load_current_state()
            # Reload categories and rules from the newly copied current directory
            self.reload_config()
            
            print(f"✓ Loaded state '{save_name}'")
            return True
        except Exception as e:
            raise e
    
    def get_available_saves(self):
        """Get list of available save states."""
        saves = []
        for save_dir in self.saves_dir.iterdir():
            if save_dir.is_dir():
                metadata_file = save_dir / "metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        saves.append({
                            'name': save_dir.name,
                            'display_name': metadata.get('save_name', save_dir.name),
                            'created_at': metadata.get('created_at', ''),
                            'original_file': metadata.get('original_file', '')
                        })
                    except:
                        # If metadata is corrupted, still show the save
                        saves.append({
                            'name': save_dir.name,
                            'display_name': save_dir.name,
                            'created_at': '',
                            'original_file': ''
                        })
        
        # Sort by creation date (newest first)
        saves.sort(key=lambda x: x['created_at'], reverse=True)
        return saves
    
    def has_current_state(self):
        """Check if there's a current working state."""
        current_transactions = self.current_dir / "transactions.csv"
        return current_transactions.exists()
    
    def get_categorization_data(self):
        """Get data formatted for web categorization interface."""
        categorization_data = {
            'expenses': [],
            'income': [],
            'savings': [],
            'ignored': []
        }
        
        # Format expenses for categorization
        if self.expenses_df is not None:
            for idx, row in self.expenses_df.iterrows():
                categorization_data['expenses'].append({
                    'id': idx,
                    'transaction_id': row.get('Transaction_ID', ''),
                    'date': str(row.get('Date', '')),
                    'narration': str(row.get('Narration', '')),
                    'amount': float(row.get('Withdrawal Amt.', 0)),
                    'category': str(row.get('Category', 'Uncategorized')),
                    'notes': str(row.get('Notes', ''))
                })
        
        # Format income for categorization
        if self.income_df is not None:
            for idx, row in self.income_df.iterrows():
                categorization_data['income'].append({
                    'id': idx,
                    'transaction_id': row.get('Transaction_ID', ''),
                    'date': str(row.get('Date', '')),
                    'narration': str(row.get('Narration', '')),
                    'amount': float(row.get('Deposit Amt.', 0)),
                    'category': str(row.get('Category', 'Uncategorized')),
                    'notes': str(row.get('Notes', ''))
                })

        # Format ignored for categorization
        if hasattr(self, 'ignored_df') and self.ignored_df is not None:
            for idx, row in self.ignored_df.iterrows():
                categorization_data['ignored'].append({
                    'id': idx,
                    'transaction_id': row.get('Transaction_ID', ''),
                    'date': str(row.get('Date', '')),
                    'narration': str(row.get('Narration', '')),
                    'amount': float(row.get('Withdrawal Amt.', row.get('Deposit Amt.', 0)) or 0),
                    'category': 'Ignored',
                    'notes': str(row.get('Notes', ''))
                })
        
        # Format savings for categorization
        if self.savings_df is not None:
            for idx, row in self.savings_df.iterrows():
                categorization_data['savings'].append({
                    'id': idx,
                    'transaction_id': row.get('Transaction_ID', ''),
                    'date': str(row.get('Date', '')),
                    'narration': str(row.get('Narration', '')),
                    'amount': float(row.get('Withdrawal Amt.', 0)),
                    'category': str(row.get('Category', 'Uncategorized')),
                    'notes': str(row.get('Notes', ''))
                })
        
        return categorization_data
    
    def update_categorization(self, transaction_id, category, notes='', action='categorize'):
        """Update transaction categorization from web interface."""
        if action == 'ignore':
            # Permanently delete the transaction from all dataframes
            return self.delete_transaction_by_id(transaction_id)
        
        return self.categorize_transaction_by_id(transaction_id, category, notes)

    def delete_transaction_by_id(self, transaction_id):
        """Delete a transaction across main, expenses, income, and savings by its ID."""
        removed_any = False
        if self.data is not None and 'Transaction_ID' in self.data.columns:
            before = len(self.data)
            self.data = self.data[self.data['Transaction_ID'] != transaction_id].reset_index(drop=True)
            removed_any = removed_any or (len(self.data) != before)
        if self.expenses_df is not None and 'Transaction_ID' in self.expenses_df.columns:
            before = len(self.expenses_df)
            self.expenses_df = self.expenses_df[self.expenses_df['Transaction_ID'] != transaction_id].reset_index(drop=True)
            removed_any = removed_any or (len(self.expenses_df) != before)
        if self.income_df is not None and 'Transaction_ID' in self.income_df.columns:
            before = len(self.income_df)
            self.income_df = self.income_df[self.income_df['Transaction_ID'] != transaction_id].reset_index(drop=True)
            removed_any = removed_any or (len(self.income_df) != before)
        if self.savings_df is not None and 'Transaction_ID' in self.savings_df.columns:
            before = len(self.savings_df)
            self.savings_df = self.savings_df[self.savings_df['Transaction_ID'] != transaction_id].reset_index(drop=True)
            removed_any = removed_any or (len(self.savings_df) != before)
        return removed_any
    

    


    def _load_categories(self):
        """Load categories from JSON file or create default ones."""
        if self.categories_file.exists():
            with open(self.categories_file, 'r') as f:
                self.categories = json.load(f)
        else:
            # Default categories
            self.categories = {
                "expense_categories": [
                    "Food & Dining", "Shopping", "Transportation", "Bills & Utilities",
                    "Healthcare", "Entertainment", "Travel", "Saving", "Investment",
                    "Transfer", "ATM", "Fees", "Others"
                ],
                "income_categories": [
                    "Salary", "Business Income", "Investment Returns", "Interest",
                    "Refund", "Gift", "Rental", "Freelance", "Others"
                ]
            }
            self._save_categories()

    def _save_categories(self):
        """Save categories to JSON file."""
        with open(self.categories_file, 'w') as f:
            json.dump(self.categories, f, indent=2)

    def add_category(self, category_name, category_type="expense"):
        """Add a new category to the list."""
        key = f"{category_type}_categories"
        if key in self.categories and category_name not in self.categories[key]:
            self.categories[key].append(category_name)
            self._save_categories()
            return True
        return False

    def get_categories(self, category_type="expense"):
        """Get list of categories by type."""
        key = f"{category_type}_categories"
        return self.categories.get(key, [])

    def reload_config(self):
        """Reload categories and rules from files."""
        self._load_categories()
        self._load_rules()
        print("✓ Configuration reloaded from files")

    def save_json_state(self):
        """Save current dataframes state with versioning to JSON format."""
        state_dir = self.config_dir / "states"
        state_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_file = state_dir / f"state_{timestamp}.json"
        
        # Save current state
        state_data = {
            "timestamp": timestamp,
            "expenses_count": len(self.expenses_df) if self.expenses_df is not None else 0,
            "income_count": len(self.income_df) if self.income_df is not None else 0,
            "expenses_data": self.expenses_df.to_dict('records') if self.expenses_df is not None else [],
            "income_data": self.income_df.to_dict('records') if self.income_df is not None else []
        }
        
        with open(state_file, 'w') as f:
            json.dump(state_data, f, indent=2, default=str)
        
        # Clean up old states (keep last 10)
        self._cleanup_old_states(state_dir)
        
        return state_file

    def _cleanup_old_states(self, state_dir):
        """Keep only the last 10 state files."""
        state_files = list(state_dir.glob("state_*.json"))
        if len(state_files) > 10:
            # Sort by timestamp and keep only the newest 10
            state_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            for old_file in state_files[10:]:
                old_file.unlink()

    def load_json_state(self, state_file):
        """Load a previous state from JSON format."""
        with open(state_file, 'r') as f:
            state_data = json.load(f)
        
        if state_data['expenses_data']:
            self.expenses_df = pd.DataFrame(state_data['expenses_data'])
        if state_data['income_data']:
            self.income_df = pd.DataFrame(state_data['income_data'])
        
        return state_data

    def get_available_states(self):
        """Get list of available state files."""
        state_dir = self.config_dir / "states"
        if not state_dir.exists():
            return []
        
        state_files = list(state_dir.glob("state_*.json"))
        return sorted(state_files, key=lambda x: x.stat().st_mtime, reverse=True)

    def _load_rules(self):
        """Load categorization rules from JSON file or create default ones."""
        if self.rules_file.exists():
            with open(self.rules_file, 'r') as f:
                self.rules_config = json.load(f)
        else:
            # Default rules
            self.rules_config = {"rules": []}
            self._save_rules()

    def _save_rules(self):
        """Save rules to JSON file."""
        with open(self.rules_file, 'w') as f:
            json.dump(self.rules_config, f, indent=2)

    def get_rules(self):
        """Get all categorization rules."""
        return self.rules_config.get("rules", [])

    def add_rule(self, rule_data):
        """Add a new categorization rule."""
        import uuid
        rule_data['id'] = f"rule_{str(uuid.uuid4())[:8]}"
        rule_data['created_date'] = datetime.now().strftime("%Y-%m-%d")
        
        if "rules" not in self.rules_config:
            self.rules_config["rules"] = []
        
        self.rules_config["rules"].append(rule_data)
        self._save_rules()
        return rule_data['id']

    def update_rule(self, rule_id, rule_data):
        """Update an existing rule."""
        for i, rule in enumerate(self.rules_config.get("rules", [])):
            if rule['id'] == rule_id:
                rule_data['id'] = rule_id
                rule_data['created_date'] = rule.get('created_date', datetime.now().strftime("%Y-%m-%d"))
                self.rules_config["rules"][i] = rule_data
                self._save_rules()
                return True
        return False

    def delete_rule(self, rule_id):
        """Delete a rule."""
        original_length = len(self.rules_config.get("rules", []))
        self.rules_config["rules"] = [rule for rule in self.rules_config.get("rules", []) if rule['id'] != rule_id]
        
        if len(self.rules_config["rules"]) < original_length:
            self._save_rules()
            return True
        return False

    def apply_rules_to_uncategorized(self):
        """Apply all enabled rules to uncategorized transactions."""
        import re
        
        applied_count = 0
        rules = [rule for rule in self.get_rules() if rule.get('enabled', True)]
        
        for rule in rules:
            # Apply to expenses
            if rule.get('transaction_type') == 'expense' and self.expenses_df is not None:
                mask = self.expenses_df['Category'] == 'Uncategorized'
                
                # Apply pattern matching
                if rule.get('pattern_type') == 'regex':
                    pattern_mask = self.expenses_df['Narration'].str.contains(rule['pattern'], na=False, case=False, regex=True)
                else:  # contains
                    pattern_mask = self.expenses_df['Narration'].str.contains(rule['pattern'], na=False, case=False)
                
                # Apply amount range filter if specified
                if rule.get('amount_min') is not None and rule.get('amount_max') is not None:
                    amount_mask = self.expenses_df['Withdrawal Amt.'].between(rule['amount_min'], rule['amount_max'])
                    final_mask = mask & pattern_mask & amount_mask
                elif rule.get('amount_min') is not None:
                    amount_mask = self.expenses_df['Withdrawal Amt.'] >= rule['amount_min']
                    final_mask = mask & pattern_mask & amount_mask
                elif rule.get('amount_max') is not None:
                    amount_mask = self.expenses_df['Withdrawal Amt.'] <= rule['amount_max']
                    final_mask = mask & pattern_mask & amount_mask
                else:
                    final_mask = mask & pattern_mask
                
                if final_mask.any():
                    count = final_mask.sum()
                    target_category = rule['category']
                    # Ensure category exists in master expense categories (exclude specials)
                    try:
                        specials = ['Uncategorized', 'Ignored', 'IGNORE', 'IGNORED', 'Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP']
                        if target_category and target_category not in specials:
                            self.add_category(target_category, 'expense')
                    except Exception:
                        pass
                    self.expenses_df.loc[final_mask, 'Category'] = target_category
                    self.expenses_df.loc[final_mask, 'Notes'] = str(rule.get('notes', ''))

                    # If rule category indicates savings, move matched rows to savings_df
                    savings_categories = ['Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP']
                    ignored_categories = ['Ignored', 'IGNORE', 'IGNORED']
                    if target_category in savings_categories:
                        moving_rows = self.expenses_df[final_mask].copy()
                        moving_rows['Category'] = 'Saving'  # normalize to Saving
                        # Update main dataframe for these Transaction_IDs so reloads recompute savings
                        if self.data is not None and 'Transaction_ID' in self.data.columns:
                            ids = moving_rows['Transaction_ID'].dropna().unique().tolist()
                            if ids:
                                main_mask = self.data['Transaction_ID'].isin(ids)
                                self.data.loc[main_mask, 'Category'] = 'Saving'
                                if 'Notes' in self.data.columns:
                                    self.data.loc[main_mask, 'Notes'] = str(rule.get('notes', ''))
                        if self.savings_df is None:
                            self.savings_df = pd.DataFrame()
                        self.savings_df = pd.concat([self.savings_df, moving_rows], ignore_index=True)
                        # Remove moved rows from expenses
                        self.expenses_df = self.expenses_df[~final_mask].reset_index(drop=True)
                    elif target_category in ignored_categories:
                        moving_rows = self.expenses_df[final_mask].copy()
                        moving_rows['Category'] = 'Ignored'
                        if self.data is not None and 'Transaction_ID' in self.data.columns:
                            ids = moving_rows['Transaction_ID'].dropna().unique().tolist()
                            if ids:
                                main_mask = self.data['Transaction_ID'].isin(ids)
                                self.data.loc[main_mask, 'Category'] = 'Ignored'
                                if 'Notes' in self.data.columns:
                                    self.data.loc[main_mask, 'Notes'] = str(rule.get('notes', ''))
                        if not hasattr(self, 'ignored_df') or self.ignored_df is None:
                            self.ignored_df = pd.DataFrame()
                        self.ignored_df = pd.concat([self.ignored_df, moving_rows], ignore_index=True)
                        self.expenses_df = self.expenses_df[~final_mask].reset_index(drop=True)
                    applied_count += count
                    print(f"✓ Applied rule '{rule['name']}' to {count} expense transaction(s)")
            
            # Apply to income
            elif rule.get('transaction_type') == 'income' and self.income_df is not None:
                mask = self.income_df['Category'] == 'Uncategorized'
                
                # Apply pattern matching
                if rule.get('pattern_type') == 'regex':
                    pattern_mask = self.income_df['Narration'].str.contains(rule['pattern'], na=False, case=False, regex=True)
                else:  # contains
                    pattern_mask = self.income_df['Narration'].str.contains(rule['pattern'], na=False, case=False)
                
                # Apply amount range filter if specified
                if rule.get('amount_min') is not None and rule.get('amount_max') is not None:
                    amount_mask = self.income_df['Deposit Amt.'].between(rule['amount_min'], rule['amount_max'])
                    final_mask = mask & pattern_mask & amount_mask
                elif rule.get('amount_min') is not None:
                    amount_mask = self.income_df['Deposit Amt.'] >= rule['amount_min']
                    final_mask = mask & pattern_mask & amount_mask
                elif rule.get('amount_max') is not None:
                    amount_mask = self.income_df['Deposit Amt.'] <= rule['amount_max']
                    final_mask = mask & pattern_mask & amount_mask
                else:
                    final_mask = mask & pattern_mask
                
                if final_mask.any():
                    count = final_mask.sum()
                    target_category = rule['category']
                    # Ensure category exists in master income categories (exclude specials)
                    try:
                        specials = ['Uncategorized', 'Ignored', 'IGNORE', 'IGNORED', 'Saving', 'Investment', 'FD', 'Mutual Fund', 'SIP']
                        if target_category and target_category not in specials:
                            self.add_category(target_category, 'income')
                    except Exception:
                        pass
                    self.income_df.loc[final_mask, 'Category'] = target_category
                    self.income_df.loc[final_mask, 'Notes'] = str(rule.get('notes', ''))

                    ignored_categories = ['Ignored', 'IGNORE', 'IGNORED']
                    if target_category in ignored_categories:
                        moving_rows = self.income_df[final_mask].copy()
                        moving_rows['Category'] = 'Ignored'
                        if self.data is not None and 'Transaction_ID' in self.data.columns:
                            ids = moving_rows['Transaction_ID'].dropna().unique().tolist()
                            if ids:
                                main_mask = self.data['Transaction_ID'].isin(ids)
                                self.data.loc[main_mask, 'Category'] = 'Ignored'
                                if 'Notes' in self.data.columns:
                                    self.data.loc[main_mask, 'Notes'] = str(rule.get('notes', ''))
                        if not hasattr(self, 'ignored_df') or self.ignored_df is None:
                            self.ignored_df = pd.DataFrame()
                        self.ignored_df = pd.concat([self.ignored_df, moving_rows], ignore_index=True)
                        self.income_df = self.income_df[~final_mask].reset_index(drop=True)
                    applied_count += count
                    print(f"✓ Applied rule '{rule['name']}' to {count} income transaction(s)")
        
        # Update main dataframe as well
        if applied_count > 0:
            # Merge back the changes
            if self.expenses_df is not None:
                expense_updates = self.expenses_df[['Transaction_ID', 'Category', 'Notes']].copy()
                self.data = self.data.set_index('Transaction_ID').combine_first(expense_updates.set_index('Transaction_ID')).reset_index()
            
            if self.income_df is not None:
                income_updates = self.income_df[['Transaction_ID', 'Category', 'Notes']].copy()
                self.data = self.data.set_index('Transaction_ID').combine_first(income_updates.set_index('Transaction_ID')).reset_index()
            
            # Persist updates to the current working directory
            self.save_current_state()
        
        return applied_count

    def move_to_ignored(self, transaction_id: str):
        """Move a transaction (from expenses or income) into ignored_df and update main data."""
        if not transaction_id:
            return False
        moved = False
        # From expenses
        if self.expenses_df is not None and 'Transaction_ID' in self.expenses_df.columns:
            mask = self.expenses_df['Transaction_ID'] == transaction_id
            if mask.any():
                rows = self.expenses_df[mask].copy()
                rows['Category'] = 'Ignored'
                if not hasattr(self, 'ignored_df') or self.ignored_df is None:
                    self.ignored_df = pd.DataFrame()
                self.ignored_df = pd.concat([self.ignored_df, rows], ignore_index=True)
                self.expenses_df = self.expenses_df[~mask].reset_index(drop=True)
                moved = True
        # From income
        if not moved and self.income_df is not None and 'Transaction_ID' in self.income_df.columns:
            mask = self.income_df['Transaction_ID'] == transaction_id
            if mask.any():
                rows = self.income_df[mask].copy()
                rows['Category'] = 'Ignored'
                if not hasattr(self, 'ignored_df') or self.ignored_df is None:
                    self.ignored_df = pd.DataFrame()
                self.ignored_df = pd.concat([self.ignored_df, rows], ignore_index=True)
                self.income_df = self.income_df[~mask].reset_index(drop=True)
                moved = True
        # Update main data
        if moved and self.data is not None and 'Transaction_ID' in self.data.columns:
            main_mask = self.data['Transaction_ID'] == transaction_id
            self.data.loc[main_mask, 'Category'] = 'Ignored'
        if moved:
            self.save_current_state()
        return moved

    def restore_from_ignored(self, transaction_id: str, target: str = 'expense', category: str = 'Uncategorized'):
        """Restore a transaction from ignored_df back to expenses or income with given category."""
        if not transaction_id:
            return False
        if not hasattr(self, 'ignored_df') or self.ignored_df is None:
            return False
        mask = self.ignored_df['Transaction_ID'] == transaction_id
        if not mask.any():
            return False
        rows = self.ignored_df[mask].copy()
        rows['Category'] = category or 'Uncategorized'
        if target == 'income':
            if self.income_df is None:
                self.income_df = pd.DataFrame()
            self.income_df = pd.concat([self.income_df, rows], ignore_index=True)
        else:
            if self.expenses_df is None:
                self.expenses_df = pd.DataFrame()
            self.expenses_df = pd.concat([self.expenses_df, rows], ignore_index=True)
        # Remove from ignored
        self.ignored_df = self.ignored_df[~mask].reset_index(drop=True)
        # Update main data
        if self.data is not None and 'Transaction_ID' in self.data.columns:
            main_mask = self.data['Transaction_ID'] == transaction_id
            self.data.loc[main_mask, 'Category'] = rows.iloc[0]['Category']
        self.save_current_state()
        return True

    def add_manual_transaction(self, date, narration, amount, txn_type='expense', category='Uncategorized', notes=''):
        import uuid
        import numpy as np
        if self.data is None:
            columns = ['Date', 'Narration', 'Chq./Ref.No.', 'Value Dt', 'Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance', 'Category', 'Transaction_ID', 'Notes']
            self.data = pd.DataFrame(columns=columns)
        txn_id = f"TXN_{str(uuid.uuid4())[:8]}"
        withdrawal = pd.to_numeric(amount, errors='coerce') if txn_type == 'expense' else np.nan
        deposit = pd.to_numeric(amount, errors='coerce') if txn_type == 'income' else np.nan
        row = {
            'Date': str(date),
            'Narration': str(narration),
            'Chq./Ref.No.': '',
            'Value Dt': '',
            'Withdrawal Amt.': withdrawal if txn_type == 'expense' else np.nan,
            'Deposit Amt.': deposit if txn_type == 'income' else np.nan,
            'Closing Balance': np.nan,
            'Category': category if category else 'Uncategorized',
            'Transaction_ID': txn_id,
            'Notes': notes or ''
        }
        self.data = pd.concat([self.data, pd.DataFrame([row])], ignore_index=True)
        self._split_expense_income()
        self.save_current_state()
        return txn_id

    def clear_all_data(self):
        """Clear all loaded data and reset to initial state."""
        self.data = None
        self.expenses_df = None
        self.income_df = None
        self.file_path = None
        print("✓ All data cleared - ready for new import")


    
    def get_analysis_data(self):
        """Get comprehensive analysis data including expenses, income, and savings."""
        analysis_data = {
            'expenses': {
                'total': 0,
                'count': 0,
                'average': 0,
                'transactions': []
            },
            'income': {
                'total': 0,
                'count': 0,
                'average': 0,
                'transactions': []
            },
            'savings': {
                'total': 0,
                'count': 0,
                'average': 0,
                'transactions': []
            },
            'summary': {
                'net_income': 0,
                'net_after_savings': 0,
                'savings_rate': 0
            }
        }
        
        # Expenses analysis
        if self.expenses_df is not None and len(self.expenses_df) > 0:
            analysis_data['expenses']['total'] = float(self.expenses_df['Withdrawal Amt.'].sum())
            analysis_data['expenses']['count'] = len(self.expenses_df)
            analysis_data['expenses']['average'] = float(self.expenses_df['Withdrawal Amt.'].mean())
            
            # Top 15 expenses
            top_expenses = self.expenses_df.nlargest(15, 'Withdrawal Amt.')
            for idx, row in top_expenses.iterrows():
                analysis_data['expenses']['transactions'].append({
                    'date': str(row.get('Date', '')),
                    'amount': float(row.get('Withdrawal Amt.', 0)),
                    'category': str(row.get('Category', 'Uncategorized')),
                    'narration': str(row.get('Narration', ''))[:60],
                    'notes': str(row.get('Notes', ''))
                })
        
        # Income analysis
        if self.income_df is not None and len(self.income_df) > 0:
            analysis_data['income']['total'] = float(self.income_df['Deposit Amt.'].sum())
            analysis_data['income']['count'] = len(self.income_df)
            analysis_data['income']['average'] = float(self.income_df['Deposit Amt.'].mean())
            
            # Top 15 income
            top_income = self.income_df.nlargest(15, 'Deposit Amt.')
            for idx, row in top_income.iterrows():
                analysis_data['income']['transactions'].append({
                    'date': str(row.get('Date', '')),
                    'amount': float(row.get('Deposit Amt.', 0)),
                    'category': str(row.get('Category', 'Uncategorized')),
                    'narration': str(row.get('Narration', ''))[:60],
                    'notes': str(row.get('Notes', ''))
                })
        
        # Savings analysis
        if self.savings_df is not None and len(self.savings_df) > 0:
            analysis_data['savings']['total'] = float(self.savings_df['Withdrawal Amt.'].sum())
            analysis_data['savings']['count'] = len(self.savings_df)
            analysis_data['savings']['average'] = float(self.savings_df['Withdrawal Amt.'].mean())
            
            # All savings transactions
            for idx, row in self.savings_df.iterrows():
                analysis_data['savings']['transactions'].append({
                    'date': str(row.get('Date', '')),
                    'amount': float(row.get('Withdrawal Amt.', 0)),
                    'category': str(row.get('Category', 'Saving')),
                    'narration': str(row.get('Narration', ''))[:60],
                    'notes': str(row.get('Notes', ''))
                })
        
        # Summary calculations
        total_income = analysis_data['income']['total']
        total_expenses = analysis_data['expenses']['total']
        total_savings = analysis_data['savings']['total']
        
        analysis_data['summary']['net_income'] = total_income - total_expenses - total_savings
        analysis_data['summary']['net_after_savings'] = total_income - total_expenses
        if total_income > 0:
            analysis_data['summary']['savings_rate'] = (total_savings / total_income) * 100
        
        return analysis_data


