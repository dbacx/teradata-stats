"""
Rules Engine for Statistics Analysis

This module implements the RulesEngine class that manages and executes
statistics analysis rules using the Strategy Pattern for maximum scalability.
"""

import logging
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
from analyzers.base_rule import BaseStatsRule

# Import all rule classes
from analyzers.rules.rule_01_unused import Rule01Unused
from analyzers.rules.rule_02_sample import Rule02Sample
from analyzers.rules.rule_03_partition_missing import Rule03PartitionMissing
from analyzers.rules.rule_04_table_missing import Rule04TableMissing
from analyzers.rules.rule_05_index_missing import Rule05IndexMissing
from analyzers.rules.rule_06_stale import Rule06Stale
from analyzers.rules.rule_07_zero_stats import Rule07ZeroStats
from analyzers.rules.rule_08_multicolumn import Rule08Multicolumn
from analyzers.rules.rule_09_skipped_sample import Rule09SkippedSample
from analyzers.rules.rule_10_dbc_missing import Rule10DBCMissing
from analyzers.rules.rule_11_redundant import Rule11Redundant
from analyzers.rules.rule_12_extrapolation import Rule12Extrapolation
from analyzers.rules.rule_13_analyze import Rule13Analyze
from analyzers.rules.rule_14_join_columns import Rule14JoinColumns
from analyzers.rules.rule_15_bloat import Rule15Bloat
from analyzers.rules.rule_16_urgent_missing import Rule16UrgentMissing

# Configure logging
logger = logging.getLogger(__name__)


class RulesEngine:
    """
    Engine for managing and executing statistics analysis rules.
    
    This class implements the Strategy Pattern by allowing registration of
    multiple rule instances and executing them in a controlled manner.
    It provides error handling, logging, and result aggregation.
    """
    
    def __init__(self):
        """Initialize the RulesEngine with empty rule registry."""
        self.rules: Dict[str, BaseStatsRule] = {}
        self.execution_history: List[Dict[str, Any]] = []
        
        logger.info("RulesEngine initialized")
    
    def register_rule(self, rule: BaseStatsRule) -> None:
        """
        Register a statistics analysis rule in the engine.
        
        Args:
            rule: Instance of BaseStatsRule to register
        
        Raises:
            ValueError: If rule_id is already registered or rule is invalid
        """
        if not isinstance(rule, BaseStatsRule):
            raise ValueError("Rule must inherit from BaseStatsRule")
        
        if rule.rule_id in self.rules:
            raise ValueError(f"Rule '{rule.rule_id}' is already registered")
        
        self.rules[rule.rule_id] = rule
        logger.info(f"Registered rule: {rule.rule_id} - {rule.rule_name}")
    
    def unregister_rule(self, rule_id: str) -> None:
        """
        Unregister a rule from the engine.
        
        Args:
            rule_id: ID of the rule to unregister
        
        Raises:
            ValueError: If rule_id is not registered
        """
        if rule_id not in self.rules:
            raise ValueError(f"Rule '{rule_id}' is not registered")
        
        rule = self.rules.pop(rule_id)
        logger.info(f"Unregistered rule: {rule.rule_id} - {rule.rule_name}")
    
    def get_rule(self, rule_id: str) -> Optional[BaseStatsRule]:
        """
        Get a registered rule by ID.
        
        Args:
            rule_id: ID of the rule to retrieve
        
        Returns:
            BaseStatsRule instance if found, None otherwise
        """
        return self.rules.get(rule_id)
    
    def register_all_rules(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Automatically register all 16 statistics analysis rules.
        
        Args:
            config: Optional dictionary with rule-specific configuration parameters.
                   Keys are rule_ids, values are parameter dictionaries.
        
        Raises:
            ValueError: If a rule fails to register
        """
        config = config or {}
        
        # Define default configurations for each rule
        rule_configs = {
            'rule_01_unused': config.get('rule_01_unused', {'days_threshold': 30}),
            'rule_02_sample': config.get('rule_02_sample', {'table_size_threshold_gb': 50.0, 'cardinality_ratio': 0.95}),
            'rule_03_partition_missing': config.get('rule_03_partition_missing', {}),
            'rule_04_table_missing': config.get('rule_04_table_missing', {}),
            'rule_05_index_missing': config.get('rule_05_index_missing', {}),
            'rule_06_stale': config.get('rule_06_stale', {'days_threshold': 15}),
            'rule_07_zero_stats': config.get('rule_07_zero_stats', {}),
            'rule_08_multicolumn': config.get('rule_08_multicolumn', {'max_value_length_threshold': 25}),
            'rule_09_skipped_sample': config.get('rule_09_skipped_sample', {}),
            'rule_10_dbc_missing': config.get('rule_10_dbc_missing', {}),
            'rule_11_redundant': config.get('rule_11_redundant', {}),
            'rule_12_extrapolation': config.get('rule_12_extrapolation', {'min_rowcount': 1000, 'min_size_gb': 1.0}),
            'rule_13_analyze': config.get('rule_13_analyze', {}),
            'rule_14_join_columns': config.get('rule_14_join_columns', {}),
            'rule_15_bloat': config.get('rule_15_bloat', {'max_stats_threshold': 50}),
            'rule_16_urgent_missing': config.get('rule_16_urgent_missing', {'min_cpu': 1000.0, 'min_freq': 20, 'min_size_gb': 10.0, 'days_back': 30})
        }
        
        # Instantiate and register each rule
        rules_to_register = [
            (Rule01Unused, rule_configs['rule_01_unused']),
            (Rule02Sample, rule_configs['rule_02_sample']),
            (Rule03PartitionMissing, rule_configs['rule_03_partition_missing']),
            (Rule04TableMissing, rule_configs['rule_04_table_missing']),
            (Rule05IndexMissing, rule_configs['rule_05_index_missing']),
            (Rule06Stale, rule_configs['rule_06_stale']),
            (Rule07ZeroStats, rule_configs['rule_07_zero_stats']),
            (Rule08Multicolumn, rule_configs['rule_08_multicolumn']),
            (Rule09SkippedSample, rule_configs['rule_09_skipped_sample']),
            (Rule10DBCMissing, rule_configs['rule_10_dbc_missing']),
            (Rule11Redundant, rule_configs['rule_11_redundant']),
            (Rule12Extrapolation, rule_configs['rule_12_extrapolation']),
            (Rule13Analyze, rule_configs['rule_13_analyze']),
            (Rule14JoinColumns, rule_configs['rule_14_join_columns']),
            (Rule15Bloat, rule_configs['rule_15_bloat']),
            (Rule16UrgentMissing, rule_configs['rule_16_urgent_missing'])
        ]
        
        registered_count = 0
        for rule_class, kwargs in rules_to_register:
            try:
                rule_instance = rule_class(**kwargs)
                self.register_rule(rule_instance)
                registered_count += 1
            except Exception as e:
                logger.error(f"Failed to register rule {rule_class.__name__}: {str(e)}")
                raise ValueError(f"Failed to register rule {rule_class.__name__}: {str(e)}")
        
        logger.info(f"Successfully registered {registered_count} rules in the engine")
    
    def list_rules(self) -> List[Dict[str, Any]]:
        """
        Get information about all registered rules.
        
        Returns:
            List of dictionaries containing rule information
        """
        return [rule.get_rule_info() for rule in self.rules.values()]
    
    def run_rule(self, rule_id: str, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Execute a single rule.
        
        Args:
            rule_id: ID of the rule to execute
            context: Dictionary containing DataFrames for analysis
        
        Returns:
            DataFrame containing the rule's analysis results
        
        Raises:
            ValueError: If rule_id is not registered
            Exception: If rule execution fails
        """
        if rule_id not in self.rules:
            raise ValueError(f"Rule '{rule_id}' is not registered")
        
        rule = self.rules[rule_id]
        start_time = datetime.now()
        
        try:
            logger.info(f"Executing rule: {rule.rule_id}")
            rule.log_analysis_start(context)
            
            # Execute the rule
            result_df = rule.analyze(context)
            
            # Log completion
            rule.log_analysis_end(result_df)
            
            # Record execution history
            execution_time = (datetime.now() - start_time).total_seconds()
            self._record_execution(rule_id, True, execution_time, len(result_df), None)
            
            logger.info(f"Rule '{rule.rule_id}' completed successfully in {execution_time:.2f}s")
            return result_df
            
        except Exception as e:
            error_msg = f"Rule '{rule.rule_id}' failed: {str(e)}"
            logger.error(error_msg)
            
            # Record failed execution
            execution_time = (datetime.now() - start_time).total_seconds()
            self._record_execution(rule_id, False, execution_time, 0, str(e))
            
            raise Exception(error_msg)
    
    def run_all(self, context: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Execute all registered rules.
        
        Args:
            context: Dictionary containing DataFrames for analysis
        
        Returns:
            Dictionary where keys are rule names and values are result DataFrames
        
        Raises:
            ValueError: If no rules are registered
        """
        if not self.rules:
            raise ValueError("No rules are registered in the engine")
        
        logger.info(f"Executing all {len(self.rules)} registered rules")
        start_time = datetime.now()
        
        results = {}
        failed_rules = []
        
        for rule_id in self.rules:
            try:
                result_df = self.run_rule(rule_id, context)
                results[rule_id] = result_df
                
            except Exception as e:
                logger.error(f"Failed to execute rule '{rule_id}': {str(e)}")
                failed_rules.append({'rule_id': rule_id, 'error': str(e)})
                # Continue with other rules even if one fails
        
        total_time = (datetime.now() - start_time).total_seconds()
        successful_count = len(results)
        
        logger.info(f"Completed rule execution: {successful_count}/{len(self.rules)} successful in {total_time:.2f}s")
        
        if failed_rules:
            logger.warning(f"Failed rules: {[rule['rule_id'] for rule in failed_rules]}")
        
        # Add execution summary to results
        results['_execution_summary'] = {
            'total_rules': len(self.rules),
            'successful_rules': successful_count,
            'failed_rules': len(failed_rules),
            'total_execution_time': total_time,
            'failed_rule_details': failed_rules
        }
        
        return results
    
    def run_selected(self, rule_ids: List[str], context: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Execute a subset of registered rules.
        
        Args:
            rule_ids: List of rule IDs to execute
            context: Dictionary containing DataFrames for analysis
        
        Returns:
            Dictionary where keys are rule names and values are result DataFrames
        
        Raises:
            ValueError: If any rule_id is not registered
        """
        # Validate all rule IDs exist
        missing_rules = [rule_id for rule_id in rule_ids if rule_id not in self.rules]
        if missing_rules:
            raise ValueError(f"Rules not registered: {missing_rules}")
        
        logger.info(f"Executing {len(rule_ids)} selected rules")
        results = {}
        
        for rule_id in rule_ids:
            try:
                result_df = self.run_rule(rule_id, context)
                results[rule_id] = result_df
                
            except Exception as e:
                logger.error(f"Failed to execute rule '{rule_id}': {str(e)}")
                # Add empty DataFrame for failed rule to maintain consistency
                results[rule_id] = pd.DataFrame()
        
        return results
    
    def get_execution_history(self) -> List[Dict[str, Any]]:
        """
        Get the execution history of all rules.
        
        Returns:
            List of dictionaries containing execution history
        """
        return self.execution_history.copy()
    
    def clear_execution_history(self) -> None:
        """Clear the execution history."""
        self.execution_history.clear()
        logger.info("Execution history cleared")
    
    def _record_execution(self, rule_id: str, success: bool, execution_time: float, 
                         result_count: int, error_message: Optional[str]) -> None:
        """
        Record execution details in history.
        
        Args:
            rule_id: ID of the executed rule
            success: Whether execution was successful
            execution_time: Time taken to execute in seconds
            result_count: Number of results returned
            error_message: Error message if execution failed
        """
        execution_record = {
            'rule_id': rule_id,
            'timestamp': datetime.now(),
            'success': success,
            'execution_time': execution_time,
            'result_count': result_count,
            'error_message': error_message
        }
        
        self.execution_history.append(execution_record)
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary of all rule executions.
        
        Returns:
            Dictionary containing performance statistics
        """
        if not self.execution_history:
            return {'message': 'No execution history available'}
        
        # Calculate statistics
        total_executions = len(self.execution_history)
        successful_executions = sum(1 for record in self.execution_history if record['success'])
        failed_executions = total_executions - successful_executions
        
        avg_execution_time = sum(record['execution_time'] for record in self.execution_history) / total_executions
        total_results = sum(record['result_count'] for record in self.execution_history)
        
        # Rule-specific statistics
        rule_stats = {}
        for record in self.execution_history:
            rule_id = record['rule_id']
            if rule_id not in rule_stats:
                rule_stats[rule_id] = {
                    'executions': 0,
                    'successes': 0,
                    'total_time': 0,
                    'total_results': 0
                }
            
            rule_stats[rule_id]['executions'] += 1
            if record['success']:
                rule_stats[rule_id]['successes'] += 1
            rule_stats[rule_id]['total_time'] += record['execution_time']
            rule_stats[rule_id]['total_results'] += record['result_count']
        
        # Calculate averages per rule
        for rule_id, stats in rule_stats.items():
            stats['success_rate'] = stats['successes'] / stats['executions']
            stats['avg_time'] = stats['total_time'] / stats['executions']
            stats['avg_results'] = stats['total_results'] / stats['executions']
        
        return {
            'total_executions': total_executions,
            'successful_executions': successful_executions,
            'failed_executions': failed_executions,
            'overall_success_rate': successful_executions / total_executions,
            'avg_execution_time': avg_execution_time,
            'total_results': total_results,
            'rule_statistics': rule_stats
        }
    
    def __len__(self) -> int:
        """Return the number of registered rules."""
        return len(self.rules)
    
    def __str__(self) -> str:
        """String representation of the engine."""
        return f"RulesEngine({len(self.rules)} rules registered)"
