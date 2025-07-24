"""
Configuration UI Module
Provides web-based interface for editing all application configurations
"""
from nicegui import ui
from typing import Dict, Any, List, Optional
import json
from database_verified_config import database_verified_config

class ConfigurationUI:
    """Web-based configuration editor"""
    
    def __init__(self):
        self.config = database_verified_config
        self.current_section = "event_categories"
        self.search_filter = ""
        
    def render_config_interface(self):
        """Render the main configuration interface"""
        with ui.card().classes('w-full'):
            ui.label('System Configuration Management').classes('text-2xl font-bold mb-4')
            
            with ui.row().classes('w-full gap-4'):
                # Left sidebar - Configuration sections
                with ui.card().classes('w-1/4 min-h-screen'):
                    self._render_config_sidebar()
                
                # Main content - Configuration editor
                with ui.card().classes('w-3/4'):
                    self._render_config_editor()
    
    def _render_config_sidebar(self):
        """Render configuration sections sidebar"""
        ui.label('Configuration Sections').classes('text-lg font-semibold mb-3')
        
        sections = [
            ("event_categories", "Event Categories", "Risk event types and scores"),
            ("event_sub_categories", "Event Sub-Categories", "Event modifiers and multipliers"),
            ("pep_types", "PEP Types", "Politically Exposed Person classifications"),
            ("entity_attributes", "Entity Attributes", "Database attribute definitions"),
            ("relationship_types", "Relationship Types", "Entity relationship classifications"),
            ("geographic_risk", "Geographic Risk", "Country-specific risk factors"),
            ("risk_thresholds", "Risk Thresholds", "Risk scoring boundaries"),
            ("system_settings", "System Settings", "Performance and system configuration"),
            ("ui_settings", "UI Settings", "User interface configuration"),
            ("database", "Database Settings", "Database connection settings"),
            ("server", "Server Settings", "Server configuration")
        ]
        
        for section_key, title, description in sections:
            with ui.card().classes('mb-2 cursor-pointer hover:bg-gray-100').on('click', 
                lambda s=section_key: self._select_section(s)):
                ui.label(title).classes('font-medium')
                ui.label(description).classes('text-sm text-gray-600')
    
    def _select_section(self, section: str):
        """Select configuration section"""
        self.current_section = section
        self._refresh_editor()
    
    def _render_config_editor(self):
        """Render the configuration editor for current section"""
        container = ui.column().classes('w-full')
        
        with container:
            # Header with search
            with ui.row().classes('w-full items-center mb-4'):
                ui.label(f'Configure {self.current_section.replace("_", " ").title()}').classes('text-xl font-semibold')
                ui.space()
                with ui.input('Search...').classes('w-64') as search_input:
                    search_input.on('input', lambda e: self._filter_configs(e.value))
                
                ui.button('Add New', on_click=self._add_new_config).props('color=primary')
                ui.button('Save All', on_click=self._save_all_configs).props('color=positive')
                ui.button('Reset', on_click=self._reset_configs).props('color=negative')
            
            # Configuration content
            self._render_section_content(container)
    
    def _render_section_content(self, container):
        """Render content for the selected section"""
        section_data = self.config.get(self.current_section, {})
        
        if self.current_section in ["event_categories", "event_sub_categories", "pep_types", 
                                    "entity_attributes", "relationship_types"]:
            self._render_code_dictionary_section(container, section_data)
        elif self.current_section == "geographic_risk":
            self._render_geographic_risk_section(container, section_data)
        elif self.current_section == "risk_thresholds":
            self._render_risk_thresholds_section(container, section_data)
        else:
            self._render_general_settings_section(container, section_data)
    
    def _render_code_dictionary_section(self, container, data: Dict):
        """Render code dictionary sections (event categories, PEP types, etc.)"""
        with container:
            # Create table headers based on section type
            if self.current_section == "event_categories":
                headers = ["Code", "Name", "Description", "Risk Score", "Severity", "Actions"]
            elif self.current_section == "event_sub_categories":
                headers = ["Code", "Name", "Description", "Multiplier", "Actions"]
            elif self.current_section == "pep_types":
                headers = ["Code", "Name", "Description", "Risk Multiplier", "Level", "Actions"]
            elif self.current_section == "entity_attributes":
                headers = ["Code", "Name", "Description", "Data Type", "Actions"]
            elif self.current_section == "relationship_types":
                headers = ["Code", "Name", "Description", "Risk Factor", "Actions"]
            else:
                headers = ["Code", "Name", "Description", "Actions"]
            
            # Create data table
            with ui.card().classes('w-full'):
                self._create_data_table(data, headers)
    
    def _create_data_table(self, data: Dict, headers: List[str]):
        """Create an editable data table"""
        table_data = []
        
        for code, info in data.items():
            if isinstance(info, dict):
                row = {"code": code}
                row.update(info)
                table_data.append(row)
        
        # Filter data if search is active
        if self.search_filter:
            table_data = [row for row in table_data 
                         if self.search_filter.lower() in str(row).lower()]
        
        # Create table
        with ui.grid(columns=len(headers)).classes('w-full gap-2'):
            # Headers
            for header in headers:
                ui.label(header).classes('font-bold text-center p-2 bg-gray-100')
            
            # Data rows
            for row in table_data:
                self._render_table_row(row, headers)
    
    def _render_table_row(self, row: Dict, headers: List[str]):
        """Render a single table row with editable fields"""
        code = row.get('code', '')
        
        # Code (non-editable)
        ui.label(code).classes('p-2 text-center')
        
        # Editable fields based on headers
        for header in headers[1:-1]:  # Skip Code and Actions
            field_key = header.lower().replace(' ', '_')
            value = row.get(field_key, '')
            
            if header in ["Risk Score", "Multiplier", "Risk Multiplier", "Risk Factor"]:
                # Numeric input
                input_elem = ui.number(value=float(value) if value else 0, step=0.1, 
                                     format='%.2f').classes('w-full')
                input_elem.on('change', lambda e, c=code, k=field_key: self._update_config_value(c, k, e.value))
            else:
                # Text input
                input_elem = ui.input(value=str(value)).classes('w-full')
                input_elem.on('change', lambda e, c=code, k=field_key: self._update_config_value(c, k, e.value))
        
        # Actions
        with ui.row().classes('gap-1'):
            ui.button('Edit', on_click=lambda c=code: self._edit_config_item(c)).props('size=sm color=primary')
            ui.button('Delete', on_click=lambda c=code: self._delete_config_item(c)).props('size=sm color=negative')
    
    def _render_geographic_risk_section(self, container, data: Dict):
        """Render geographic risk configuration"""
        with container:
            ui.label('Geographic Risk Configuration').classes('text-lg font-semibold mb-4')
            
            risk_levels = ["critical_risk", "high_risk", "medium_risk", "low_risk"]
            
            with ui.tabs().classes('w-full') as tabs:
                for level in risk_levels:
                    ui.tab(level.replace('_', ' ').title(), name=level)
            
            with ui.tab_panels(tabs, value=risk_levels[0]).classes('w-full'):
                for level in risk_levels:
                    with ui.tab_panel(level):
                        level_data = data.get(level, {})
                        self._render_country_risk_table(level, level_data)
    
    def _render_country_risk_table(self, level: str, data: Dict):
        """Render country risk configuration table"""
        with ui.card().classes('w-full'):
            ui.label(f'{level.replace("_", " ").title()} Countries').classes('font-semibold mb-2')
            
            # Add new country button
            with ui.row().classes('mb-4'):
                new_country_code = ui.input('Country Code').classes('w-20')
                new_country_name = ui.input('Country Name').classes('w-40')
                new_multiplier = ui.number('Multiplier', value=1.0, step=0.1).classes('w-24')
                new_reason = ui.input('Reason').classes('w-60')
                ui.button('Add Country', 
                         on_click=lambda: self._add_country_risk(level, 
                                                               new_country_code.value,
                                                               new_country_name.value,
                                                               new_multiplier.value,
                                                               new_reason.value)).props('color=primary')
            
            # Countries table
            with ui.grid(columns=6).classes('w-full gap-2'):
                # Headers
                for header in ["Code", "Name", "Multiplier", "Reason", "Actions"]:
                    ui.label(header).classes('font-bold text-center p-2 bg-gray-100')
                
                # Data rows
                for country_code, country_info in data.items():
                    self._render_country_row(level, country_code, country_info)
    
    def _render_country_row(self, level: str, country_code: str, country_info: Dict):
        """Render a country risk configuration row"""
        ui.label(country_code).classes('p-2 text-center')
        
        # Editable fields
        name_input = ui.input(value=country_info.get('name', '')).classes('w-full')
        multiplier_input = ui.number(value=country_info.get('multiplier', 1.0), step=0.1).classes('w-full')
        reason_input = ui.input(value=country_info.get('reason', '')).classes('w-full')
        
        # Update handlers
        name_input.on('change', lambda e: self._update_country_risk(level, country_code, 'name', e.value))
        multiplier_input.on('change', lambda e: self._update_country_risk(level, country_code, 'multiplier', e.value))
        reason_input.on('change', lambda e: self._update_country_risk(level, country_code, 'reason', e.value))
        
        # Actions
        ui.button('Delete', on_click=lambda: self._delete_country_risk(level, country_code)).props('size=sm color=negative')
    
    def _render_risk_thresholds_section(self, container, data: Dict):
        """Render risk thresholds configuration"""
        with container:
            ui.label('Risk Scoring Thresholds').classes('text-lg font-semibold mb-4')
            
            with ui.grid(columns=6).classes('w-full gap-4'):
                # Headers
                for header in ["Level", "Min Score", "Max Score", "Color", "Description", "Actions"]:
                    ui.label(header).classes('font-bold text-center p-2 bg-gray-100')
                
                # Data rows
                for level, threshold_info in data.items():
                    self._render_threshold_row(level, threshold_info)
    
    def _render_threshold_row(self, level: str, threshold_info: Dict):
        """Render a risk threshold configuration row"""
        ui.label(level.title()).classes('p-2 text-center')
        
        # Editable fields
        min_input = ui.number(value=threshold_info.get('min', 0)).classes('w-full')
        max_input = ui.number(value=threshold_info.get('max', 100)).classes('w-full')
        
        # Color picker
        with ui.row().classes('items-center'):
            color_input = ui.input(value=threshold_info.get('color', '#000000')).classes('w-20')
            ui.element('div').style(f'width: 30px; height: 30px; background-color: {threshold_info.get("color", "#000000")}; border: 1px solid #ccc;')
        
        desc_input = ui.input(value=threshold_info.get('description', '')).classes('w-full')
        
        # Update handlers
        min_input.on('change', lambda e: self._update_threshold(level, 'min', e.value))
        max_input.on('change', lambda e: self._update_threshold(level, 'max', e.value))
        color_input.on('change', lambda e: self._update_threshold(level, 'color', e.value))
        desc_input.on('change', lambda e: self._update_threshold(level, 'description', e.value))
        
        # Actions
        ui.button('Reset', on_click=lambda: self._reset_threshold(level)).props('size=sm color=secondary')
    
    def _render_general_settings_section(self, container, data: Dict):
        """Render general settings sections"""
        with container:
            ui.label(f'{self.current_section.replace("_", " ").title()} Configuration').classes('text-lg font-semibold mb-4')
            
            with ui.grid(columns=2).classes('w-full gap-4'):
                for key, value in data.items():
                    ui.label(key.replace('_', ' ').title()).classes('font-medium')
                    
                    if isinstance(value, bool):
                        switch = ui.switch(value=value)
                        switch.on('change', lambda e, k=key: self._update_general_setting(k, e.value))
                    elif isinstance(value, (int, float)):
                        number_input = ui.number(value=value)
                        number_input.on('change', lambda e, k=key: self._update_general_setting(k, e.value))
                    else:
                        text_input = ui.input(value=str(value))
                        text_input.on('change', lambda e, k=key: self._update_general_setting(k, e.value))
    
    # Event handlers
    def _update_config_value(self, code: str, field: str, value: Any):
        """Update a configuration value"""
        current_config = self.config.get(self.current_section, {})
        if code in current_config:
            current_config[code][field] = value
            self.config.set(self.current_section, current_config)
    
    def _update_country_risk(self, level: str, country_code: str, field: str, value: Any):
        """Update country risk configuration"""
        path = f"geographic_risk.{level}.{country_code}.{field}"
        self.config.set(path, value)
    
    def _update_threshold(self, level: str, field: str, value: Any):
        """Update risk threshold configuration"""
        path = f"risk_thresholds.{level}.{field}"
        self.config.set(path, value)
    
    def _update_general_setting(self, key: str, value: Any):
        """Update general setting"""
        path = f"{self.current_section}.{key}"
        self.config.set(path, value)
    
    def _add_new_config(self):
        """Add new configuration item"""
        with ui.dialog() as dialog, ui.card():
            ui.label('Add New Configuration Item').classes('text-h6')
            key_input = ui.input('Configuration Key', placeholder='e.g., new_feature.enabled')
            value_input = ui.input('Configuration Value', placeholder='e.g., true')
            type_select = ui.select(['string', 'number', 'boolean', 'list'], label='Value Type', value='string')
            
            with ui.row():
                ui.button('Add', on_click=lambda: self._save_new_config(key_input.value, value_input.value, type_select.value, dialog))
                ui.button('Cancel', on_click=dialog.close)
        dialog.open()
    
    def _edit_config_item(self, code: str):
        """Edit configuration item"""
        try:
            current_value = self.config_manager.get(code, '')
            with ui.dialog() as dialog, ui.card():
                ui.label(f'Edit: {code}').classes('text-h6')
                value_input = ui.input('Value', value=str(current_value))
                
                with ui.row():
                    ui.button('Save', on_click=lambda: self._save_edited_config(code, value_input.value, dialog))
                    ui.button('Cancel', on_click=dialog.close)
            dialog.open()
        except Exception as e:
            ui.notify(f'Error editing {code}: {str(e)}', type='negative')
    
    def _delete_config_item(self, code: str):
        """Delete configuration item"""
        current_config = self.config.get(self.current_section, {})
        if code in current_config:
            del current_config[code]
            self.config.set(self.current_section, current_config)
            ui.notify(f'Deleted {code}', type='positive')
            self._refresh_editor()
    
    def _add_country_risk(self, level: str, code: str, name: str, multiplier: float, reason: str):
        """Add new country risk configuration"""
        if code and name:
            path = f"geographic_risk.{level}.{code}"
            self.config.set(path, {
                "name": name,
                "multiplier": multiplier,
                "reason": reason
            })
            ui.notify(f'Added {name} to {level}', type='positive')
            self._refresh_editor()
    
    def _delete_country_risk(self, level: str, country_code: str):
        """Delete country risk configuration"""
        current_config = self.config.get(f"geographic_risk.{level}", {})
        if country_code in current_config:
            del current_config[country_code]
            self.config.set(f"geographic_risk.{level}", current_config)
            ui.notify(f'Deleted {country_code}', type='positive')
            self._refresh_editor()
    
    def _reset_threshold(self, level: str):
        """Reset risk threshold to default"""
        ui.notify(f'Reset {level} threshold to default', type='info')
    
    def _filter_configs(self, search_term: str):
        """Filter configurations based on search term"""
        self.search_filter = search_term
        self._refresh_editor()
    
    def _save_all_configs(self):
        """Save all configuration changes"""
        self.config.save_config()
        ui.notify('Configuration saved successfully!', type='positive')
    
    def _reset_configs(self):
        """Reset configurations to defaults"""
        with ui.dialog() as dialog, ui.card():
            ui.label('Are you sure you want to reset all configurations to defaults?')
            with ui.row():
                ui.button('Yes', on_click=lambda: self._confirm_reset(dialog)).props('color=negative')
                ui.button('Cancel', on_click=dialog.close).props('color=secondary')
        dialog.open()
    
    def _confirm_reset(self, dialog):
        """Confirm configuration reset"""
        # Reset current section to defaults
        self.config.config[self.current_section] = self.config._get_comprehensive_defaults()[self.current_section]
        ui.notify(f'Reset {self.current_section} to defaults', type='positive')
        dialog.close()
        self._refresh_editor()
    
    def _save_new_config(self, key, value, value_type, dialog):
        """Save new configuration item"""
        try:
            # Convert value based on type
            if value_type == 'boolean':
                processed_value = value.lower() in ['true', '1', 'yes', 'on']
            elif value_type == 'number':
                processed_value = float(value) if '.' in value else int(value)
            elif value_type == 'list':
                processed_value = [item.strip() for item in value.split(',')]
            else:
                processed_value = value
            
            # Set the configuration
            path = f"{self.current_section}.{key}"
            self.config.set(path, processed_value)
            ui.notify(f'Added configuration: {key}', type='positive')
            dialog.close()
            self._refresh_editor()
        except Exception as e:
            ui.notify(f'Error adding configuration: {str(e)}', type='negative')
    
    def _save_edited_config(self, key, value, dialog):
        """Save edited configuration item"""
        try:
            # Try to preserve the original type
            original_value = self.config.get(key, '')
            if isinstance(original_value, bool):
                processed_value = value.lower() in ['true', '1', 'yes', 'on']
            elif isinstance(original_value, (int, float)):
                processed_value = type(original_value)(value)
            elif isinstance(original_value, list):
                processed_value = [item.strip() for item in value.split(',')]
            else:
                processed_value = value
            
            self.config.set(key, processed_value)
            ui.notify(f'Updated configuration: {key}', type='positive')
            dialog.close()
            self._refresh_editor()
        except Exception as e:
            ui.notify(f'Error updating configuration: {str(e)}', type='negative')
    
    def _refresh_editor(self):
        """Refresh the configuration editor"""
        try:
            # Clear and rebuild the main container
            if hasattr(self, 'main_container'):
                self.main_container.clear()
                with self.main_container:
                    self._build_sections()
            ui.notify('Configuration refreshed successfully', type='positive')
        except Exception as e:
            ui.notify(f'Error refreshing configuration: {str(e)}', type='negative')

# Global configuration UI instance
config_ui = ConfigurationUI()