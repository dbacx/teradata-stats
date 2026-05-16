"""
Module 6 Security Analyzer

Analyzes collected security data and generates findings with severity levels
and DDL remediation statements for the User & Security Management module.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional
from core.base_analyzer import BaseAnalyzer, Severity
from core.config import THRESHOLDS

logger = logging.getLogger(__name__)


class SecurityAnalyzer(BaseAnalyzer):
    """
    Analyzer for Security Assessment Module (Module 10).
    
    Analyzes data from 5 components to identify security issues:
    1. Password Expiry - Users with expired passwords
    2. Users Without Profile - Users without assigned profiles
    3. Direct Grants - Users with direct access rights instead of roles
    4. Stagnant Users - Users with no recent access
    5. Users Without Role - Productive users not in any role
    """
    
    def __init__(self):
        """Initialize the Security Analyzer."""
        super().__init__(
            analyzer_name='SecurityAnalyzer',
            description='Analyzes Teradata user and security for optimization opportunities'
        )
        logger.info("Initialized SecurityAnalyzer")
    
    def run(self, data: Dict[str, pd.DataFrame], config: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
        """
        Run analysis on collected security data.
        
        Args:
            data: Dictionary mapping component names to DataFrames
            config: Optional configuration with thresholds
        
        Returns:
            Dictionary mapping component names to analyzed DataFrames with findings
        """
        # Use config thresholds if provided, otherwise use defaults
        if config is None:
            config = THRESHOLDS.copy()
        
        self.clear_findings()
        analyzed_results = {}
        
        # Analyze each component
        analyzed_results['01_password_expiry'] = self._analyze_password_expiry(
            data.get('01_password_expiry', pd.DataFrame())
        )
        
        analyzed_results['02_users_without_profile'] = self._analyze_users_without_profile(
            data.get('02_users_without_profile', pd.DataFrame())
        )
        
        analyzed_results['03_direct_grants'] = self._analyze_direct_grants(
            data.get('03_direct_grants', pd.DataFrame())
        )
        
        analyzed_results['04_stagnant_users'] = self._analyze_stagnant_users(
            data.get('04_stagnant_users', pd.DataFrame())
        )
        
        analyzed_results['05_users_without_role'] = self._analyze_users_without_role(
            data.get('05_users_without_role', pd.DataFrame())
        )
        
        logger.info(f"Analysis complete. Total findings: {len(self.findings)}")
        return analyzed_results
    
    def _analyze_password_expiry(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze password expiry.
        
        Severity: CRITICAL if > 180 days, HIGH if > 90 days, MEDIUM if > 60 days
        DDL: MODIFY USER ... AS PASSWORD ... EXPIRE PASSWORD
        """
        if df.empty:
            logger.warning("DataFrame is empty for 01_password_expiry")
            return df
        
        required_columns = ['UserName', 'PasswordLastModDate', 'DaysSincePasswordChange']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 01_password_expiry. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'PASSWORD_EXPIRY'
        result_df['Description'] = 'User password has not been changed recently'
        result_df['DDL_Action'] = 'EXPIRE_PASSWORD'
        
        for idx, row in result_df.iterrows():
            days_since = row.get('DaysSincePasswordChange', 0)
            
            if days_since > 180:
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='PASSWORD_EXPIRY',
                    description=f"CRITICAL: User {row['UserName']} password has not been changed in {days_since} days (last: {row['PasswordLastModDate']})",
                    metadata={
                        'username': row['UserName'],
                        'creator_name': row['CreatorName'],
                        'days_since_change': days_since,
                        'last_mod_date': str(row['PasswordLastModDate'])
                    }
                )
            elif days_since > 90:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='PASSWORD_EXPIRY',
                    description=f"User {row['UserName']} password has not been changed in {days_since} days (last: {row['PasswordLastModDate']})",
                    metadata={
                        'username': row['UserName'],
                        'creator_name': row['CreatorName'],
                        'days_since_change': days_since,
                        'last_mod_date': str(row['PasswordLastModDate'])
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='PASSWORD_EXPIRY',
                    description=f"User {row['UserName']} password has not been changed in {days_since} days",
                    metadata={
                        'username': row['UserName'],
                        'days_since_change': days_since
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"MODIFY USER {x['UserName']} AS PASSWORD 'NewTempPass123!' EXPIRE PASSWORD;",
            axis=1
        )
        
        return result_df
    
    def _analyze_users_without_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze users without profile.
        
        Severity: HIGH (users without security profile)
        DDL: MODIFY USER ... AS PROFILE "DEFAULT_PROFILE"
        """
        if df.empty:
            logger.warning("DataFrame is empty for 02_users_without_profile")
            return df
        
        required_columns = ['UserName', 'CreatorName', 'DefaultDatabase']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 02_users_without_profile. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'HIGH'
        result_df['Finding_Type'] = 'USER_WITHOUT_PROFILE'
        result_df['Description'] = 'User does not have an assigned profile'
        result_df['DDL_Action'] = 'ASSIGN_PROFILE'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.HIGH,
                finding_type='USER_WITHOUT_PROFILE',
                description=f"User {row['UserName']} (created by {row['CreatorName']}) has no profile assigned (default DB: {row['DefaultDatabase']})",
                metadata={
                    'username': row['UserName'],
                    'creator_name': row['CreatorName'],
                    'default_database': row['DefaultDatabase']
                }
            )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"MODIFY USER {x['UserName']} AS PROFILE 'DEFAULT_PROFILE';",
            axis=1
        )
        
        return result_df
    
    def _analyze_direct_grants(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze direct grants (users with direct access instead of roles).
        
        Severity: MEDIUM (should use role-based access)
        DDL: REVOKE ... and GRANT through role
        """
        if df.empty:
            logger.warning("DataFrame is empty for 03_direct_grants")
            return df
        
        required_columns = ['UserName', 'DatabaseName', 'AccessRight']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 03_direct_grants. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'DIRECT_GRANT'
        result_df['Description'] = 'User has direct access rights instead of role-based access'
        result_df['DDL_Action'] = 'USE_ROLES'
        
        for idx, row in result_df.iterrows():
            table_name = row.get('TableName', 'ALL')
            self.add_finding(
                severity=Severity.MEDIUM,
                finding_type='DIRECT_GRANT',
                description=f"User {row['UserName']} has direct {row['AccessRight']} on {row['DatabaseName']}.{table_name} (granted by {row['GrantorName']})",
                metadata={
                    'username': row['UserName'],
                    'database': row['DatabaseName'],
                    'table': table_name,
                    'access_right': row['AccessRight'],
                    'grantor': row['GrantorName']
                }
            )
        
        # Generate DDL (revoke direct grant, recommend role-based)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"REVOKE {x['AccessRight']} ON {x['DatabaseName']}.{x.get('TableName', 'ALL')} FROM {x['UserName']}; -- Consider granting through role",
            axis=1
        )
        
        return result_df
    
    def _analyze_stagnant_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze stagnant users (no recent access).
        
        Severity: CRITICAL if > 180 days, HIGH if > 90 days, MEDIUM if > 60 days
        DDL: REVOKE LOGON ON ALL FROM user
        """
        if df.empty:
            logger.warning("DataFrame is empty for 04_stagnant_users")
            return df
        
        required_columns = ['UserName', 'LastAccessTimeStamp', 'DaysSinceLastAccess']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 04_stagnant_users. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'MEDIUM'
        result_df['Finding_Type'] = 'STAGNANT_USER'
        result_df['Description'] = 'User has not accessed the system recently'
        result_df['DDL_Action'] = 'REVOKE_LOGON'
        
        for idx, row in result_df.iterrows():
            days_since = row.get('DaysSinceLastAccess', 0)
            
            if days_since > 180:
                result_df.at[idx, 'Severity'] = 'CRITICAL'
                self.add_finding(
                    severity=Severity.CRITICAL,
                    finding_type='STAGNANT_USER',
                    description=f"CRITICAL: User {row['UserName']} has not accessed the system in {days_since} days (last: {row['LastAccessTimeStamp']})",
                    metadata={
                        'username': row['UserName'],
                        'creator_name': row['CreatorName'],
                        'days_since_access': days_since,
                        'last_access': str(row['LastAccessTimeStamp'])
                    }
                )
            elif days_since > 90:
                result_df.at[idx, 'Severity'] = 'HIGH'
                self.add_finding(
                    severity=Severity.HIGH,
                    finding_type='STAGNANT_USER',
                    description=f"User {row['UserName']} has not accessed the system in {days_since} days (last: {row['LastAccessTimeStamp']})",
                    metadata={
                        'username': row['UserName'],
                        'creator_name': row['CreatorName'],
                        'days_since_access': days_since,
                        'last_access': str(row['LastAccessTimeStamp'])
                    }
                )
            else:
                self.add_finding(
                    severity=Severity.MEDIUM,
                    finding_type='STAGNANT_USER',
                    description=f"User {row['UserName']} has not accessed the system in {days_since} days",
                    metadata={
                        'username': row['UserName'],
                        'days_since_access': days_since
                    }
                )
        
        # Generate DDL
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"REVOKE LOGON ON ALL FROM {x['UserName']};",
            axis=1
        )
        
        return result_df
    
    def _analyze_users_without_role(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze users without role membership.
        
        Severity: LOW (informational, should use roles for better management)
        DDL: GRANT role TO user
        """
        if df.empty:
            logger.warning("DataFrame is empty for 05_users_without_role")
            return df
        
        required_columns = ['UserName', 'CreatorName', 'DefaultDatabase']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"Missing required columns for 05_users_without_role. Required: {required_columns}, Available: {df.columns.tolist()}")
            return df
        
        result_df = df.copy()
        result_df['Severity'] = 'LOW'
        result_df['Finding_Type'] = 'USER_WITHOUT_ROLE'
        result_df['Description'] = 'Productive user is not a member of any role'
        result_df['DDL_Action'] = 'ASSIGN_ROLE'
        
        for idx, row in result_df.iterrows():
            self.add_finding(
                severity=Severity.LOW,
                finding_type='USER_WITHOUT_ROLE',
                description=f"User {row['UserName']} (created by {row['CreatorName']}) is not a member of any role (default DB: {row['DefaultDatabase']})",
                metadata={
                    'username': row['UserName'],
                    'creator_name': row['CreatorName'],
                    'default_database': row['DefaultDatabase']
                }
            )
        
        # Generate DDL (recommend assigning to a role)
        result_df['DDL_Statement'] = result_df.apply(
            lambda x: f"GRANT 'DEFAULT_ROLE' TO {x['UserName']}; -- Review appropriate role assignment",
            axis=1
        )
        
        return result_df
