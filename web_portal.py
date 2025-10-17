#!/usr/bin/env python3
"""
Web Portal for Transaction Categorization

A Flask web application to manage transaction categorization.
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import os
import sys
import pandas as pd
from transaction_processor import AccountStatementProcessor

app = Flask(__name__)
processor = None

# Initialize empty processor
def init_processor():
    global processor
    processor = AccountStatementProcessor()
    print("📊 Transaction Processor initialized - ready for file upload")

# Initialize on startup (but don't auto-load files)
init_processor()

@app.route('/')
def index():
    """Main dashboard showing categorization statistics."""
    # Ensure we don't redirect to upload if a current working state exists
    if processor is None:
        return render_template('upload.html')
    if processor.data is None:
        try:
            # If a current state exists, attempt to load it
            if processor.has_current_state():
                # Reload current state into memory
                processor._load_current_state()
            else:
                return render_template('upload.html')
        except Exception:
            return render_template('upload.html')
    
    # Get basic statistics
    total_transactions = len(processor.data) if processor.data is not None else 0
    total_expenses = len(processor.expenses_df) if processor.expenses_df is not None else 0
    total_income = len(processor.income_df) if processor.income_df is not None else 0
    
    # Get categorization statistics
    categorized_expenses = 0
    uncategorized_expenses = 0
    if processor.expenses_df is not None:
        categorized_expenses = len(processor.expenses_df[processor.expenses_df['Category'] != 'Uncategorized'])
        uncategorized_expenses = len(processor.expenses_df[processor.expenses_df['Category'] == 'Uncategorized'])
    
    categorized_income = 0
    uncategorized_income = 0
    if processor.income_df is not None:
        categorized_income = len(processor.income_df[processor.income_df['Category'] != 'Uncategorized'])
        uncategorized_income = len(processor.income_df[processor.income_df['Category'] == 'Uncategorized'])
    
    stats = {
        'total_transactions': total_transactions,
        'total_expenses': total_expenses,
        'total_income': total_income,
        'categorized_expenses': categorized_expenses,
        'uncategorized_expenses': uncategorized_expenses,
        'categorized_income': categorized_income,
        'uncategorized_income': uncategorized_income
    }
    
    return render_template('dashboard.html', stats=stats)

@app.route('/load_file', methods=['POST'])
def load_file():
    """Load account statement file."""
    global processor
    
    file_path = request.form.get('file_path')
    if not file_path or not os.path.exists(file_path):
        return jsonify({'success': False, 'error': 'File not found'})
    
    try:
        processor = AccountStatementProcessor()
        processor.load_file(file_path)
        processor.apply_initial_categorizations()
        
        # Save initial state
        processor.save_json_state()
        
        return jsonify({'success': True, 'message': 'File loaded successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/monthly_chart_data')
def api_monthly_chart_data():
    """Return month-wise totals by category for expenses and income for stacked bar charts.
    Response shape:
      {
        success: true,
        expenses: { labels: ["YYYY-MM", ...], datasets: [{label: category, data: [...]}, ...] },
        income: { labels: [...], datasets: [...] }
      }
    """
    global processor
    try:
        if processor is None:
            return jsonify({'success': False, 'error': 'No data loaded'})

        def build_monthly(df, amount_col, exclude_ignore=True):
            if df is None or len(df) == 0:
                return {'labels': [], 'datasets': []}
            dff = df.copy()
            # Normalize
            dff[amount_col] = pd.to_numeric(dff[amount_col], errors='coerce').fillna(0)
            if exclude_ignore:
                dff = dff[dff['Category'] != 'Ignore']
            # Parse month key from Date
            def to_month(s):
                try:
                    # Expect bank-like DD/MM/YY or DD/MM/YYYY
                    parts = str(s).split('/')
                    if len(parts) == 3:
                        dd, mm, yy = [p.strip() for p in parts]
                        if len(yy) == 2:
                            y = int(yy)
                            y = 1900 + y if y >= 70 else 2000 + y
                        else:
                            y = int(yy)
                        return f"{y:04d}-{int(mm):02d}"
                except Exception:
                    pass
                # Fallback to pandas to_datetime
                try:
                    dt = pd.to_datetime(s)
                    return dt.strftime('%Y-%m')
                except Exception:
                    return 'Unknown'

            dff['Month'] = dff['Date'].apply(to_month)
            grouped = dff.groupby(['Month', 'Category'])[amount_col].sum().reset_index()
            # Build pivot Month x Category
            pivot = grouped.pivot(index='Month', columns='Category', values=amount_col).fillna(0)
            # Sort months
            labels = sorted(pivot.index.tolist())
            pivot = pivot.loc[labels]
            # Build datasets per category
            datasets = []
            for cat in pivot.columns.tolist():
                datasets.append({
                    'label': str(cat),
                    'data': [float(v) for v in pivot[cat].tolist()]
                })
            return {'labels': labels, 'datasets': datasets}

        expenses_payload = build_monthly(processor.expenses_df, 'Withdrawal Amt.')
        income_payload = build_monthly(processor.income_df, 'Deposit Amt.')

        return jsonify({'success': True, 'expenses': expenses_payload, 'income': income_payload})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/move_to_ignored', methods=['POST'])
def api_move_to_ignored():
    """Move a transaction (by Transaction_ID) into ignored_df."""
    global processor
    try:
        data = request.get_json() or {}
        txn_id = data.get('transaction_id')
        if not txn_id:
            return jsonify({'success': False, 'error': 'transaction_id is required'})
        moved = processor.move_to_ignored(txn_id)
        return jsonify({'success': bool(moved)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/move_from_ignored', methods=['POST'])
def api_move_from_ignored():
    """Restore a transaction from ignored_df back to expenses or income."""
    global processor
    try:
        data = request.get_json() or {}
        txn_id = data.get('transaction_id')
        target = data.get('target', 'expense')  # 'expense' or 'income'
        category = data.get('category', 'Uncategorized')
        if not txn_id:
            return jsonify({'success': False, 'error': 'transaction_id is required'})
        restored = processor.restore_from_ignored(txn_id, target=target, category=category)
        return jsonify({'success': bool(restored)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/ignored')
def ignored():
    """Show ignored transactions with ability to restore."""
    global processor
    if processor is None:
        return redirect(url_for('index'))
    # Build simple list from ignored_df
    items = []
    if hasattr(processor, 'ignored_df') and processor.ignored_df is not None:
        df = processor.ignored_df.copy()
        for _, row in df.iterrows():
            items.append({
                'transaction_id': row.get('Transaction_ID', ''),
                'date': str(row.get('Date', '')),
                'narration': str(row.get('Narration', '')),
                'amount': row.get('Withdrawal Amt.', row.get('Deposit Amt.', 0))
            })
    return render_template('ignored.html', items=items)
@app.route('/api/chart_data')
def get_chart_data():
    """Return aggregated category totals for expenses and income for charts."""
    global processor
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    try:
        expense_data = {}
        income_data = {}
        # Aggregate expenses by category
        if processor.expenses_df is not None and len(processor.expenses_df) > 0:
            df = processor.expenses_df.copy()
            # ensure numeric
            df['Withdrawal Amt.'] = pd.to_numeric(df['Withdrawal Amt.'], errors='coerce')
            grouped = df.groupby('Category')['Withdrawal Amt.'].sum().dropna()
            expense_data = {str(k): float(v) for k, v in grouped.items()}
        # Aggregate income by category
        if processor.income_df is not None and len(processor.income_df) > 0:
            df = processor.income_df.copy()
            df['Deposit Amt.'] = pd.to_numeric(df['Deposit Amt.'], errors='coerce')
            grouped = df.groupby('Category')['Deposit Amt.'].sum().dropna()
            income_data = {str(k): float(v) for k, v in grouped.items()}
        return jsonify({'success': True, 'expense_data': expense_data, 'income_data': income_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/expenses')
def expenses():
    """Show expenses for categorization."""
    if processor is None or processor.expenses_df is None:
        return redirect(url_for('index'))
    
    # Sorting
    sort_order = request.args.get('sort', 'desc')
    df = processor.expenses_df.copy()
    if sort_order == 'asc':
        df = df.sort_values('Withdrawal Amt.', ascending=True)
    else:
        df = df.sort_values('Withdrawal Amt.', ascending=False)
    
    # Get expenses data
    expenses_data = []
    for idx, row in df.iterrows():
        expenses_data.append({
            'id': idx,
            'transaction_id': row.get('Transaction_ID', ''),
            'date': str(row.get('Date', '')),
            'narration': str(row.get('Narration', ''))[:100],  # Truncate for display
            'amount': f"₹{row.get('Withdrawal Amt.', 0):,.0f}",
            'amount_value': row.get('Withdrawal Amt.', 0),  # For sorting
            'category': str(row.get('Category', 'Uncategorized')),
            'notes': str(row.get('Notes', ''))
        })
    
    # Get categories from file-based configuration
    categories = processor.get_categories('expense')
    
    return render_template('expenses.html', expenses=expenses_data, categories=categories, sort_order=sort_order)

@app.route('/income')
def income():
    """Show income for categorization."""
    if processor is None or processor.income_df is None:
        return redirect(url_for('index'))
    
    # Sorting
    sort_order = request.args.get('sort', 'desc')
    df = processor.income_df.copy()
    if sort_order == 'asc':
        df = df.sort_values('Deposit Amt.', ascending=True)
    else:
        df = df.sort_values('Deposit Amt.', ascending=False)
    
    # Get income data
    income_data = []
    for idx, row in df.iterrows():
        income_data.append({
            'id': idx,
            'transaction_id': row.get('Transaction_ID', ''),
            'date': str(row.get('Date', '')),
            'narration': str(row.get('Narration', ''))[:100],  # Truncate for display
            'amount': f"₹{row.get('Deposit Amt.', 0):,.0f}",
            'amount_value': row.get('Deposit Amt.', 0),  # For sorting
            'category': str(row.get('Category', 'Uncategorized')),
            'notes': str(row.get('Notes', ''))
        })
    
    # Get categories from file-based configuration
    categories = processor.get_categories('income')
    
    return render_template('income.html', income=income_data, categories=categories, sort_order=sort_order)

@app.route('/savings')
def savings():
    """Show savings transactions."""
    if processor is None:
        return redirect(url_for('index'))
    
    # Check if savings_df exists and has data
    if processor.savings_df is None or processor.savings_df.empty:
        savings_data = []
    else:
        # Sorting
        sort_order = request.args.get('sort', 'desc')
        df = processor.savings_df.copy()
        if sort_order == 'asc':
            df = df.sort_values('Withdrawal Amt.', ascending=True)
        else:
            df = df.sort_values('Withdrawal Amt.', ascending=False)
        
        # Get savings data
        savings_data = []
        for idx, row in df.iterrows():
            savings_data.append({
                'id': idx,
                'transaction_id': row.get('Transaction_ID', ''),
                'date': str(row.get('Date', '')),
                'narration': str(row.get('Narration', ''))[:100],  # Truncate for display
                'amount': f"₹{row.get('Withdrawal Amt.', 0):,.0f}",
                'amount_value': row.get('Withdrawal Amt.', 0),  # For sorting
                'category': str(row.get('Category', 'Saving')),
                'notes': str(row.get('Notes', ''))
            })
    
    return render_template('savings.html', savings=savings_data, sort_order=request.args.get('sort', 'desc'))

@app.route('/categorize', methods=['POST'])
def categorize():
    """Update transaction categorization."""
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    
    data = request.get_json()
    transaction_id = data.get('transaction_id')
    category = data.get('category')
    notes = data.get('notes', '')
    action = data.get('action', 'categorize')
    
    if not transaction_id:
        return jsonify({'success': False, 'error': 'Transaction ID required'})
    
    try:
        success = processor.update_categorization(transaction_id, category, notes, action)
        
        if success:
            # Persist updates to current working state
            processor.save_current_state()
            return jsonify({'success': True, 'message': 'Transaction updated successfully'})
        else:
            return jsonify({'success': False, 'error': 'Transaction not found'})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/mark_saving', methods=['POST'])
def mark_saving():
    """Mark a transaction as saving (move from expenses to savings)."""
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    
    data = request.get_json()
    transaction_id = data.get('transaction_id')
    action = data.get('action', 'mark_saving')
    
    if not transaction_id:
        return jsonify({'success': False, 'error': 'Transaction ID required'})
    
    try:
        if action == 'mark_saving':
            success = processor.mark_as_saving(transaction_id)
        elif action == 'unmark_saving':
            category = data.get('category', 'Uncategorized')
            success = processor.move_from_saving_to_expense(transaction_id, category)
        else:
            return jsonify({'success': False, 'error': 'Invalid action'})
        
        if success:
            processor.save_current_state()
            return jsonify({'success': True, 'message': 'Transaction updated successfully'})
        else:
            return jsonify({'success': False, 'error': 'Transaction not found'})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/create_category', methods=['POST'])
def create_category():
    """Create a new category."""
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    
    data = request.get_json()
    category_name = data.get('category_name', '').strip()
    
    if not category_name:
        return jsonify({'success': False, 'error': 'Category name required'})
    
    # Add validation for category name
    if len(category_name) > 50:
        return jsonify({'success': False, 'error': 'Category name too long (max 50 characters)'})
    
    if category_name in ['Uncategorized', 'IGNORED']:
        return jsonify({'success': False, 'error': 'Reserved category name'})
    
    try:
        # The category will be available next time the page loads
        # No need to store separately as it will be part of the transaction data
        return jsonify({'success': True, 'message': f'Category "{category_name}" ready to use'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/add_category', methods=['POST'])
def add_category():
    """Add a new category."""
    try:
        data = request.get_json()
        category_name = data.get('category_name', '').strip()
        category_type = data.get('category_type', 'expense')
        
        if not category_name:
            return jsonify({'success': False, 'error': 'Category name cannot be empty'})
        
        success = processor.add_category(category_name, category_type)
        if success:
            return jsonify({'success': True, 'categories': processor.get_categories(category_type)})
        else:
            return jsonify({'success': False, 'error': 'Category already exists'})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/get_categories/<category_type>')
def get_categories_api(category_type):
    """Get categories by type."""
    try:
        categories = processor.get_categories(category_type)
        return jsonify({'success': True, 'categories': categories})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/graphs')
def graphs():
    """Show graphs and analytics based on categories."""
    if processor is None or processor.expenses_df is None:
        return redirect(url_for('index'))
    
    return render_template('graphs.html')
@app.route('/api/chart_data')
def chart_data():
    """Get chart data for categories."""
    try:
        if processor is None:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        # Expense category data
        expense_data = {}
        if processor.expenses_df is not None:
            # Group by category and sum amounts
            expense_groups = processor.expenses_df[processor.expenses_df['Category'] != 'Ignore'].groupby('Category')['Withdrawal Amt.'].sum()
            expense_data = expense_groups.to_dict()
        
        # Income category data  
        income_data = {}
        if processor.income_df is not None:
            income_groups = processor.income_df[processor.income_df['Category'] != 'Ignore'].groupby('Category')['Deposit Amt.'].sum()
            income_data = income_groups.to_dict()
        
        return jsonify({
            'success': True,
            'expense_data': expense_data,
            'income_data': income_data
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle CSV file upload and start new analysis with blank state."""
    global processor
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    
    if file and file.filename.lower().endswith(('.csv', '.xls', '.xlsx')):
        try:
            # Save uploaded file temporarily
            import tempfile
            suffix = '.csv' if file.filename.lower().endswith('.csv') else '.xlsx'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                file.save(temp_file.name)
                
                # Initialize new processor with fresh blank state
                # Clear current working directory so it seeds categories/rules from defaults
                import shutil
                tmp_proc = AccountStatementProcessor()
                if tmp_proc.current_dir.exists():
                    shutil.rmtree(tmp_proc.current_dir)
                tmp_proc.current_dir.mkdir(parents=True, exist_ok=True)
                # Recreate processor which seeds categories/rules in current dir from defaults
                processor = AccountStatementProcessor()
                processor.load_file(temp_file.name)
                
                # Apply initial categorization with fresh state
                processor.apply_initial_categorizations()
                # Persist to current working directory
                processor.save_current_state()
                
                # Clean up temp file
                os.unlink(temp_file.name)
                
                return jsonify({
                    'success': True,
                    'message': f'File uploaded successfully! New analysis started with default categories and rules.',
                    'data': {
                        'total_transactions': len(processor.data),
                        'expenses': len(processor.expenses_df) if processor.expenses_df is not None else 0,
                        'income': len(processor.income_df) if processor.income_df is not None else 0
                    }
                })
                
        except Exception as e:
            return jsonify({'success': False, 'error': f'Error processing file: {str(e)}'})
    
    return jsonify({'success': False, 'error': 'Invalid file format. Please upload .csv, .xls or .xlsx files only.'})

@app.route('/load_existing')
def load_existing():
    """Show load states page with available saved states."""
    states = []
    saves_dir = './saves'
    
    if os.path.exists(saves_dir):
        for state_name in os.listdir(saves_dir):
            state_path = os.path.join(saves_dir, state_name)
            if os.path.isdir(state_path):
                # Get state info
                state_info = {
                    'name': state_name,
                    'timestamp': 'Unknown',
                    'expenses_count': 0,
                    'income_count': 0,
                    'savings_count': 0
                }
                
                # Try to get file modification time
                try:
                    import time
                    mtime = os.path.getmtime(state_path)
                    state_info['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                except:
                    pass
                
                # Try to get transaction counts from CSV files
                try:
                    import pandas as pd
                    expenses_file = os.path.join(state_path, 'expenses.csv')
                    income_file = os.path.join(state_path, 'income.csv')
                    savings_file = os.path.join(state_path, 'savings.csv')
                    
                    if os.path.exists(expenses_file):
                        df = pd.read_csv(expenses_file)
                        state_info['expenses_count'] = len(df)
                    
                    if os.path.exists(income_file):
                        df = pd.read_csv(income_file)
                        state_info['income_count'] = len(df)
                        
                    if os.path.exists(savings_file):
                        df = pd.read_csv(savings_file)
                        state_info['savings_count'] = len(df)
                except:
                    pass
                
                states.append(state_info)
    
    return render_template('load_states.html', states=states)

@app.route('/api/load_state/<state_name>', methods=['POST'])
def load_state(state_name):
    """Load a saved state by copying it into current working directory."""
    global processor
    try:
        if processor is None:
            processor = AccountStatementProcessor()
        processor.load_named_state(state_name)
        return jsonify({'success': True, 'message': f'State "{state_name}" loaded into current working directory'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})



@app.route('/rules')
def rules_management():
    """Show rules management page."""
    if processor is None:
        return redirect(url_for('index'))
    
    return render_template('rules.html')

@app.route('/api/rules', methods=['GET', 'POST'])
def rules_api():
    """Get or save categorization rules."""
    if request.method == 'GET':
        try:
            rules = processor.get_rules()
            return jsonify({'success': True, 'rules': rules})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    elif request.method == 'POST':
        try:
            data = request.get_json()
            # Accept both nested {rule: {...}} and plain {...}
            rule_data = None
            if isinstance(data, dict):
                rule_data = data.get('rule', data)
            if not rule_data or not isinstance(rule_data, dict):
                return jsonify({'success': False, 'error': 'Rule data required'})

            rule_id = processor.add_rule(rule_data)
            return jsonify({'success': True, 'message': 'Rule added successfully', 'rule_id': rule_id})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

@app.route('/api/rules/<rule_id>', methods=['PUT', 'DELETE'])
def rule_individual(rule_id):
    """Update or delete individual rules."""
    if request.method == 'PUT':
        try:
            rule_data = request.get_json()
            if not rule_data:
                return jsonify({'success': False, 'error': 'Rule data required'})
            
            success = processor.update_rule(rule_id, rule_data)
            if success:
                return jsonify({'success': True, 'message': 'Rule updated successfully'})
            else:
                return jsonify({'success': False, 'error': 'Rule not found'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    elif request.method == 'DELETE':
        try:
            success = processor.delete_rule(rule_id)
            if success:
                return jsonify({'success': True, 'message': 'Rule deleted successfully'})
            else:
                return jsonify({'success': False, 'error': 'Rule not found'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

@app.route('/api/apply_rules', methods=['POST'])
def apply_rules():
    """Apply all enabled rules to uncategorized transactions."""
    try:
        if processor is None or processor.data is None:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        applied_count = processor.apply_rules_to_uncategorized()
        return jsonify({'success': True, 'applied_count': applied_count})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/save_state', methods=['POST'])
def save_state():
    """Save current working directory as a named state (copy current -> saves/<name>)."""
    global processor
    
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'State name is required'})
    
    try:
        processor.save_named_state(name)
        return jsonify({'success': True, 'message': f'State saved as "{name}"'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/reset_current', methods=['POST'])
def reset_current():
    """Reset the application by clearing the current working directory."""
    global processor
    try:
        import shutil
        # Clear current directory safely
        if processor is None:
            processor = AccountStatementProcessor()
        if processor.current_dir.exists():
            shutil.rmtree(processor.current_dir)
        processor.current_dir.mkdir(parents=True, exist_ok=True)
        # Reset in-memory data
        processor.data = None
        processor.expenses_df = None
        processor.income_df = None
        processor.savings_df = None
        return jsonify({'success': True, 'message': 'Application reset. Current working directory cleared.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/analysis_data')
def get_analysis_data():
    """Get comprehensive analysis data including expenses, income, and savings."""
    global processor
    
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    
    try:
        analysis_data = processor.get_analysis_data()
        return jsonify({'success': True, 'data': analysis_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/category_details')
def api_category_details():
    """Return category-wise transactions and totals for analysis page.
    Query params:
      - type: one of 'expenses', 'income', 'savings'
      - category: category name to filter by
    """
    global processor
    try:
        if processor is None:
            return jsonify({'success': False, 'error': 'No data loaded'})

        cat_type = request.args.get('type', 'expenses').strip().lower()
        category = request.args.get('category', '').strip()
        if not category:
            return jsonify({'success': False, 'error': 'category is required'})

        # Determine dataframe and amount column
        df = None
        amount_col = None
        if cat_type == 'expenses':
            df = processor.expenses_df
            amount_col = 'Withdrawal Amt.'
        elif cat_type == 'income':
            df = processor.income_df
            amount_col = 'Deposit Amt.'
        elif cat_type == 'savings':
            df = processor.savings_df
            amount_col = 'Withdrawal Amt.'
        else:
            return jsonify({'success': False, 'error': 'invalid type'})

        if df is None or len(df) == 0:
            return jsonify({'success': True, 'data': {'total': 0.0, 'count': 0, 'transactions': []}})

        # Normalize amount column to numeric and filter by Category
        dff = df.copy()
        dff[amount_col] = pd.to_numeric(dff[amount_col], errors='coerce').fillna(0)
        dff = dff[dff['Category'] == category]

        # Build transactions list
        txns = []
        for _, row in dff.iterrows():
            txns.append({
                'transaction_id': row.get('Transaction_ID', ''),
                'date': str(row.get('Date', '')),
                'amount': float(row.get(amount_col, 0) or 0),
                'narration': str(row.get('Narration', '')),
                'notes': str(row.get('Notes', ''))
            })

        total = float(dff[amount_col].sum()) if len(dff) else 0.0
        return jsonify({'success': True, 'data': {
            'total': total,
            'count': len(txns),
            'transactions': txns
        }})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/add_transaction', methods=['POST'])
def add_transaction():
    """Add a manual transaction (expense or income) and persist to current working state."""
    global processor
    if processor is None:
        return jsonify({'success': False, 'error': 'No data loaded'})
    try:
        data = request.get_json() or {}
        date = data.get('date')
        narration = data.get('narration')
        amount = data.get('amount')
        txn_type = data.get('txn_type', 'expense')
        category = data.get('category', 'Uncategorized')
        notes = data.get('notes', '')

        if not date or not narration or amount is None:
            return jsonify({'success': False, 'error': 'date, narration and amount are required'})

        txn_id = processor.add_manual_transaction(date, narration, amount, txn_type, category, notes)
        # Ensure state persisted in method; return updated counts for quick UI refresh
        summary = {
            'total_transactions': len(processor.data) if processor.data is not None else 0,
            'expenses': len(processor.expenses_df) if processor.expenses_df is not None else 0,
            'income': len(processor.income_df) if processor.income_df is not None else 0
        }
        return jsonify({'success': True, 'transaction_id': txn_id, 'summary': summary})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/analysis')
def analysis_page():
    """Show detailed analysis page."""
    if processor is None:
        return redirect(url_for('index'))
    
    return render_template('analysis.html')

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    print("🌐 Starting Transaction Categorization Portal...")
    print("📂 Access at: http://localhost:5000")
    print("🔧 Use Ctrl+C to stop the server")
    
    app.run(host='0.0.0.0', port=5000, debug=True)