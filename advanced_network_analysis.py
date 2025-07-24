"""
Advanced Network Analysis Module
Enterprise relationship network analysis with real entity data integration
Production-ready with actual entity relationship mapping and graph analytics
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from nicegui import ui
from collections import defaultdict, Counter
from datetime import datetime

# Required dependencies
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class AdvancedNetworkAnalysis:
    """Enterprise Network Analysis with Real Entity Data Integration"""
    
    def __init__(self):
        self.current_network = None
        self.current_entities = []
        self.network_stats = {}
        self.community_detection = {}
        self.centrality_measures = {}
        
    def create_advanced_network_interface(self):
        """Create enterprise network analysis interface"""
        try:
            with ui.column().classes('w-full gap-4 p-4'):
                self._create_header()
                
                # Main content tabs
                with ui.tabs().classes('w-full') as network_tabs:
                    visualization_tab = ui.tab('🕸️ Network Visualization', icon='hub')
                    analysis_tab = ui.tab('📊 Graph Analytics', icon='analytics')  
                    community_tab = ui.tab('👥 Community Detection', icon='groups')
                    centrality_tab = ui.tab('🎯 Centrality Analysis', icon='center_focus_strong')
                
                with ui.tab_panels(network_tabs, value=visualization_tab).classes('w-full'):
                    with ui.tab_panel(visualization_tab):
                        self._create_network_visualization_panel()
                    
                    with ui.tab_panel(analysis_tab):
                        self._create_graph_analytics_panel()
                    
                    with ui.tab_panel(community_tab):
                        self._create_community_detection_panel()
                    
                    with ui.tab_panel(centrality_tab):
                        self._create_centrality_analysis_panel()
                        
        except Exception as e:
            logger.error(f"Error creating network analysis interface: {e}")
            ui.label(f"Error creating network analysis interface: {str(e)}").classes('text-red-500')

    def _create_header(self):
        """Create interface header with real connection status"""
        with ui.card().classes('w-full p-4 bg-gradient-to-r from-purple-600 to-indigo-600 text-white'):
            with ui.row().classes('w-full items-center justify-between'):
                with ui.column():
                    ui.label('Advanced Network Analysis').classes('text-h5 font-bold')
                    ui.label('Real-time entity relationship mapping and graph analytics').classes('text-sm opacity-90')
                
                with ui.row().classes('gap-2'):
                    self.status_badge = ui.badge('Ready', color='green').classes('px-3 py-1')
                    self.last_update = ui.label('Never analyzed').classes('text-xs opacity-80')

    def _create_network_visualization_panel(self):
        """Create interactive network visualization panel with real data"""
        with ui.column().classes('w-full gap-4'):
            # Network generation controls
            with ui.card().classes('w-full p-4'):
                ui.label('Network Generation & Visualization').classes('text-h6 font-bold mb-3')
                
                with ui.row().classes('gap-2'):
                    ui.button('Generate Network from Current Results', 
                             icon='hub',
                             on_click=self._generate_network_from_results).classes('bg-purple-600 text-white')
                    
                    ui.button('Clear Network', 
                             icon='clear',
                             on_click=self._clear_network).classes('bg-gray-600 text-white')
                
                with ui.row().classes('gap-4 mt-4'):
                    self.layout_select = ui.select(
                        label='Layout Algorithm',
                        options=['spring', 'circular', 'kamada_kawai', 'random', 'shell'],
                        value='spring'
                    ).classes('flex-1')
                    
                    self.max_nodes_input = ui.number(
                        label='Max Nodes',
                        value=50,
                        min=10,
                        max=200
                    ).classes('w-32')
            
            # Interactive network visualization
            with ui.card().classes('w-full'):
                ui.label('Interactive Network Graph').classes('text-h6 font-bold mb-3')
                self.network_container = ui.column().classes('w-full h-96')
                self._create_empty_network_display()
            
            # Network statistics
            with ui.card().classes('w-full'):
                ui.label('Network Statistics').classes('text-h6 font-bold mb-3')
                self.stats_container = ui.column().classes('w-full')
                self._update_network_stats_display()

    def _create_graph_analytics_panel(self):
        """Create graph analytics panel with real metrics"""
        with ui.column().classes('w-full gap-4'):
            # Graph metrics overview
            with ui.card().classes('w-full p-4'):
                ui.label('Graph Metrics Overview').classes('text-h6 font-bold mb-3')
                
                with ui.row().classes('gap-8'):
                    with ui.column().classes('text-center'):
                        ui.label('Nodes').classes('text-sm font-bold text-gray-600')
                        self.nodes_count_label = ui.label('--').classes('text-2xl font-bold text-purple-600')
                    
                    with ui.column().classes('text-center'):
                        ui.label('Edges').classes('text-sm font-bold text-gray-600')
                        self.edges_count_label = ui.label('--').classes('text-2xl font-bold text-blue-600')
                    
                    with ui.column().classes('text-center'):
                        ui.label('Density').classes('text-sm font-bold text-gray-600')
                        self.density_label = ui.label('--').classes('text-2xl font-bold text-green-600')
                    
                    with ui.column().classes('text-center'):
                        ui.label('Components').classes('text-sm font-bold text-gray-600')
                        self.components_label = ui.label('--').classes('text-2xl font-bold text-orange-600')
            
            # Detailed analytics
            with ui.card().classes('w-full'):
                ui.label('Detailed Graph Analytics').classes('text-h6 font-bold mb-3')
                self.analytics_container = ui.column().classes('w-full')
                self._create_analytics_display()

    def _create_community_detection_panel(self):
        """Create community detection panel with real algorithms"""
        with ui.column().classes('w-full gap-4'):
            # Community detection controls
            with ui.card().classes('w-full p-4'):
                ui.label('Community Detection').classes('text-h6 font-bold mb-3')
                
                with ui.row().classes('gap-2'):
                    ui.button('Detect Communities', 
                             icon='groups',
                             on_click=self._detect_communities).classes('bg-indigo-600 text-white')
                    
                    self.algorithm_select = ui.select(
                        label='Algorithm',
                        options=['louvain', 'greedy_modularity', 'label_propagation'],
                        value='louvain'
                    ).classes('flex-1')
            
            # Community results
            with ui.card().classes('w-full'):
                ui.label('Community Detection Results').classes('text-h6 font-bold mb-3')
                self.community_container = ui.column().classes('w-full')
                self._create_community_display()

    def _create_centrality_analysis_panel(self):
        """Create centrality analysis panel with real calculations"""
        with ui.column().classes('w-full gap-4'):
            # Centrality controls
            with ui.card().classes('w-full p-4'):
                ui.label('Centrality Analysis').classes('text-h6 font-bold mb-3')
                
                with ui.row().classes('gap-2'):
                    ui.button('Calculate Centralities', 
                             icon='center_focus_strong',
                             on_click=self._calculate_centralities).classes('bg-teal-600 text-white')
                    
                    self.centrality_select = ui.select(
                        label='Centrality Measure',
                        options=['degree', 'betweenness', 'closeness', 'eigenvector', 'pagerank'],
                        value='degree'
                    ).classes('flex-1')
            
            # Centrality results
            with ui.card().classes('w-full'):
                ui.label('Centrality Analysis Results').classes('text-h6 font-bold mb-3')
                self.centrality_container = ui.column().classes('w-full')
                self._create_centrality_display()

    def _generate_network_from_results(self):
        """Generate network from current search results with real entity data"""
        try:
            self.status_badge.text = 'Generating...'
            self.status_badge.color = 'orange'
            
            # Get current search results from main application with fallback options
            from main import UserSessionManager
            user_app_instance, user_id = UserSessionManager.get_user_app_instance()
            
            # Try multiple sources for search results to ensure session persistence
            current_results = (
                getattr(user_app_instance, 'current_results', []) or 
                getattr(user_app_instance, 'filtered_data', []) or 
                getattr(user_app_instance, 'last_search_results', [])
            )
            
            if not current_results:
                ui.notify('No search results found. Please perform a search first, then return to the Network Analysis tab.', type='warning')
                self.status_badge.text = 'No Data'
                self.status_badge.color = 'red'
                return
            
            # Generate network graph from real entity data
            self.current_entities = current_results
            self.current_network = self._build_network_graph(current_results)
            
            # Update visualization with real data
            self._update_network_visualization()
            self._update_network_stats_display()
            
            self.status_badge.text = 'Success'
            self.status_badge.color = 'green'
            self.last_update.text = f"Last analyzed: {datetime.now().strftime('%H:%M:%S')}"
            
            ui.notify(f'Network generated with {self.current_network.number_of_nodes()} nodes and {self.current_network.number_of_edges()} edges', type='positive')
            
        except Exception as e:
            logger.error(f"Error generating network: {e}")
            self.status_badge.text = 'Error'
            self.status_badge.color = 'red'
            ui.notify(f'Network generation failed: {str(e)}', type='negative')

    def _build_network_graph(self, entities: List[Dict]) -> nx.Graph:
        """Build network graph from real entity data using risk_id as primary identifier"""
        G = nx.Graph()
        
        # Create entity_id to risk_id mapping for relationship resolution
        entity_to_risk_mapping = {}
        risk_to_entity_mapping = {}
        
        # Add nodes using risk_id as primary identifier
        for entity in entities:
            risk_id = entity.get('risk_id', '')
            entity_id = entity.get('entity_id', '')
            entity_name = entity.get('entity_name', '')
            
            if risk_id:  # Use risk_id as the static identifier
                G.add_node(risk_id, 
                          name=entity_name,
                          entity_type=entity.get('entity_type', 'Unknown'),
                          risk_score=entity.get('risk_score', 0),
                          is_pep=entity.get('is_pep', False),
                          country=entity.get('primary_country', ''),
                          events_count=entity.get('events_count', 0),
                          entity_id=entity_id)  # Keep entity_id for reference
                
                # Build mapping tables for relationship resolution
                if entity_id:
                    entity_to_risk_mapping[entity_id] = risk_id
                    risk_to_entity_mapping[risk_id] = entity_id
        
        # Add edges based on real relationships data
        for entity in entities:
            source_risk_id = entity.get('risk_id', '')
            if not source_risk_id:
                continue
                
            # Parse relationships from real data
            relationships = entity.get('relationships', [])
            
            if isinstance(relationships, str):
                # Handle JSON string format
                import json
                try:
                    relationships = json.loads(relationships)
                except:
                    relationships = []
            
            if isinstance(relationships, list):
                for rel in relationships:
                    if isinstance(rel, dict):
                        related_entity_id = rel.get('related_entity_id', '')
                        rel_type = rel.get('type', 'Related')
                        direction = rel.get('direction', 'Unknown')
                        
                        # Map related_entity_id to its corresponding risk_id
                        related_risk_id = entity_to_risk_mapping.get(related_entity_id)
                        
                        # If we can't find the related entity in our current results,
                        # we need to create a placeholder node or skip it
                        if not related_risk_id:
                            # Check if we have the related entity name to create a basic node
                            related_name = rel.get('related_entity_name', '')
                            if related_name and related_entity_id:
                                # Create a minimal node for the related entity using entity_id as fallback
                                G.add_node(related_entity_id,
                                          name=related_name,
                                          entity_type='External',
                                          risk_score=0,
                                          is_pep=False,
                                          country='',
                                          events_count=0,
                                          entity_id=related_entity_id,
                                          is_external=True)  # Mark as external entity
                                related_risk_id = related_entity_id
                        
                        if related_risk_id and related_risk_id in G.nodes():
                            # Add edge with relationship metadata
                            G.add_edge(source_risk_id, related_risk_id, 
                                     relationship_type=rel_type,
                                     direction=direction,
                                     weight=1.0,
                                     is_formal_relationship=True)
            
            # Also create edges based on common attributes for entities in our dataset
            entity_country = entity.get('primary_country', '')
            entity_events = entity.get('events', [])
            
            if entity_country or entity_events:
                for other_entity in entities:
                    other_risk_id = other_entity.get('risk_id', '')
                    if other_risk_id and other_risk_id != source_risk_id and other_risk_id in G.nodes():
                        
                        # Create edges for entities from same country (weaker connection)
                        if entity_country and entity_country == other_entity.get('primary_country', ''):
                            if not G.has_edge(source_risk_id, other_risk_id):
                                G.add_edge(source_risk_id, other_risk_id, 
                                         relationship_type='Same Country',
                                         direction='Bidirectional',
                                         weight=0.3,
                                         is_formal_relationship=False)
                        
                        # Create edges for entities with similar risk profiles
                        entity_risk = entity.get('risk_score', 0)
                        other_risk = other_entity.get('risk_score', 0)
                        if entity_risk >= 70 and other_risk >= 70 and abs(entity_risk - other_risk) <= 20:
                            if not G.has_edge(source_risk_id, other_risk_id):
                                G.add_edge(source_risk_id, other_risk_id, 
                                         relationship_type='Similar Risk Profile',
                                         direction='Bidirectional',
                                         weight=0.2,
                                         is_formal_relationship=False)
        
        return G

    def _update_network_visualization(self):
        """Update network visualization using Plotly with real data"""
        self.network_container.clear()
        
        if not self.current_network or self.current_network.number_of_nodes() == 0:
            with self.network_container:
                ui.label('No network data available. Generate a network first.').classes('text-gray-500 italic text-center p-8')
            return
        
        try:
            # Get layout positions
            layout_type = self.layout_select.value
            max_nodes = int(self.max_nodes_input.value)
            
            # Limit nodes for performance
            if self.current_network.number_of_nodes() > max_nodes:
                degrees = dict(self.current_network.degree())
                top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:max_nodes]
                subgraph_nodes = [node[0] for node in top_nodes]
                G = self.current_network.subgraph(subgraph_nodes).copy()
            else:
                G = self.current_network
            
            # Calculate layout
            if layout_type == 'spring':
                pos = nx.spring_layout(G, k=1, iterations=50)
            elif layout_type == 'circular':
                pos = nx.circular_layout(G)
            elif layout_type == 'kamada_kawai':
                pos = nx.kamada_kawai_layout(G) if G.number_of_nodes() < 100 else nx.spring_layout(G)
            elif layout_type == 'shell':
                pos = nx.shell_layout(G)
            else:
                pos = nx.random_layout(G)
            
            # Create Plotly traces for edges
            edge_x = []
            edge_y = []
            edge_info = []
            
            for edge in G.edges(data=True):
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
                
                rel_type = edge[2].get('relationship_type', 'Unknown')
                edge_info.append(f"Relationship: {rel_type}")
            
            edge_trace = go.Scatter(x=edge_x, y=edge_y,
                                  line=dict(width=1, color='rgba(125,125,125,0.5)'),
                                  hoverinfo='none',
                                  mode='lines')
            
            # Create Plotly traces for nodes with real entity data
            node_x = []
            node_y = []
            node_text = []
            node_colors = []
            node_sizes = []
            hover_text = []
            
            for node in G.nodes(data=True):
                node_id = node[0]
                node_data = node[1]
                x, y = pos[node_id]
                node_x.append(x)
                node_y.append(y)
                
                # Real entity information
                name = node_data.get('name', node_id)
                entity_type = node_data.get('entity_type', 'Unknown')
                risk_score = node_data.get('risk_score', 0)
                is_pep = node_data.get('is_pep', False)
                country = node_data.get('country', '')
                events_count = node_data.get('events_count', 0)
                
                # Truncate name for display
                display_name = name[:15] + "..." if len(name) > 15 else name
                node_text.append(display_name)
                
                # Detailed hover information
                hover_info = f"<b>{name}</b><br>"
                hover_info += f"Type: {entity_type}<br>"
                hover_info += f"Risk Score: {risk_score}<br>"
                hover_info += f"PEP Status: {'Yes' if is_pep else 'No'}<br>"
                if country:
                    hover_info += f"Country: {country}<br>"
                hover_info += f"Events: {events_count}<br>"
                hover_info += f"Connections: {G.degree(node_id)}"
                hover_text.append(hover_info)
                
                # Color by risk level and PEP status
                if is_pep:
                    node_colors.append('#FF4444')  # Red for PEPs
                elif risk_score >= 80:
                    node_colors.append('#FF8C00')  # Orange for high risk
                elif risk_score >= 50:
                    node_colors.append('#FFD700')  # Gold for medium risk
                else:
                    node_colors.append('#87CEEB')  # Light blue for low risk
                
                # Size by degree centrality and risk score
                degree = G.degree(node_id)
                base_size = 20
                degree_size = min(degree * 3, 20)
                risk_size = min(risk_score / 10, 15)
                node_sizes.append(base_size + degree_size + risk_size)
            
            node_trace = go.Scatter(x=node_x, y=node_y,
                                  mode='markers+text',
                                  hoverinfo='text',
                                  text=node_text,
                                  textposition="middle center",
                                  hovertext=hover_text,
                                  marker=dict(size=node_sizes,
                                            color=node_colors,
                                            line=dict(width=2, color='white'),
                                            opacity=0.8))
            
            # Create figure
            fig = go.Figure(data=[edge_trace, node_trace],
                          layout=go.Layout(
                                title=f'Entity Relationship Network ({G.number_of_nodes()} entities, {G.number_of_edges()} relationships)',
                                titlefont_size=16,
                                showlegend=False,
                                hovermode='closest',
                                margin=dict(b=20,l=5,r=5,t=40),
                                annotations=[ dict(
                                    text="Interactive network - hover for details, zoom and pan to explore",
                                    showarrow=False,
                                    xref="paper", yref="paper",
                                    x=0.005, y=-0.002,
                                    xanchor="left", yanchor="bottom",
                                    font=dict(color="gray", size=12)
                                )],
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                plot_bgcolor='white'))
            
            with self.network_container:
                ui.plotly(fig).classes('w-full h-96')
                
        except Exception as e:
            logger.error(f"Error updating network visualization: {e}")
            with self.network_container:
                ui.label(f'Error creating visualization: {str(e)}').classes('text-red-500')

    def _create_empty_network_display(self):
        """Create empty network display placeholder"""
        with self.network_container:
            ui.label('No network generated yet. Click "Generate Network from Current Results" to create a network visualization from your search results.').classes('text-gray-500 italic text-center p-8')

    def _update_network_stats_display(self):
        """Update network statistics display with real calculations"""
        self.stats_container.clear()
        
        if not self.current_network:
            with self.stats_container:
                ui.label('No network statistics available').classes('text-gray-500 italic')
            return
        
        try:
            # Calculate real network statistics
            num_nodes = self.current_network.number_of_nodes()
            num_edges = self.current_network.number_of_edges()
            
            if num_nodes > 1:
                density = nx.density(self.current_network)
                components = nx.number_connected_components(self.current_network)
                avg_degree = sum(dict(self.current_network.degree()).values()) / num_nodes
                
                # Calculate additional real metrics
                clustering_coeff = nx.average_clustering(self.current_network)
                diameter = 0
                try:
                    if nx.is_connected(self.current_network):
                        diameter = nx.diameter(self.current_network)
                except:
                    diameter = 0
            else:
                density = 0
                components = 1 if num_nodes > 0 else 0
                avg_degree = 0
                clustering_coeff = 0
                diameter = 0
            
            with self.stats_container:
                stats_data = [
                    ['Total Entities', num_nodes],
                    ['Relationships', num_edges],
                    ['Network Density', f'{density:.3f}'],
                    ['Connected Components', components],
                    ['Average Connections', f'{avg_degree:.1f}'],
                    ['Clustering Coefficient', f'{clustering_coeff:.3f}'],
                    ['Network Diameter', diameter if diameter > 0 else 'N/A']
                ]
                
                for stat_name, stat_value in stats_data:
                    with ui.row().classes('justify-between items-center'):
                        ui.label(stat_name).classes('font-medium')
                        ui.badge(str(stat_value), color='blue').classes('px-2 py-1')
            
            # Update header counters
            self.nodes_count_label.text = str(num_nodes)
            self.edges_count_label.text = str(num_edges)
            self.density_label.text = f'{density:.3f}'
            self.components_label.text = str(components)
            
        except Exception as e:
            logger.error(f"Error updating network stats: {e}")
            with self.stats_container:
                ui.label(f'Error calculating statistics: {str(e)}').classes('text-red-500')

    def _clear_network(self):
        """Clear current network"""
        self.current_network = None
        self.current_entities = []
        self.network_stats = {}
        self.community_detection = {}
        self.centrality_measures = {}
        
        self.network_container.clear()
        self._create_empty_network_display()
        self._update_network_stats_display()
        
        self.status_badge.text = 'Ready'
        self.status_badge.color = 'green'
        
        ui.notify('Network cleared', type='positive')

    def _create_analytics_display(self):
        """Create detailed analytics display"""
        with self.analytics_container:
            ui.label('Generate a network first to see detailed analytics').classes('text-gray-500 italic')

    def _create_community_display(self):
        """Create community detection display"""
        with self.community_container:
            ui.label('Generate a network and click "Detect Communities" to see community analysis').classes('text-gray-500 italic')

    def _create_centrality_display(self):
        """Create centrality analysis display"""
        with self.centrality_container:
            ui.label('Generate a network and click "Calculate Centralities" to see centrality analysis').classes('text-gray-500 italic')

    def _detect_communities(self):
        """Detect communities in the network using real algorithms"""
        if not self.current_network:
            ui.notify('Generate a network first', type='warning')
            return
        
        try:
            algorithm = self.algorithm_select.value
            
            if algorithm == 'louvain':
                import networkx.algorithms.community as nx_comm
                communities = list(nx_comm.louvain_communities(self.current_network))
                modularity = nx_comm.modularity(self.current_network, communities)
            elif algorithm == 'greedy_modularity':
                import networkx.algorithms.community as nx_comm
                communities = list(nx_comm.greedy_modularity_communities(self.current_network))
                modularity = nx_comm.modularity(self.current_network, communities)
            else:  # label_propagation
                import networkx.algorithms.community as nx_comm
                communities = list(nx_comm.label_propagation_communities(self.current_network))
                modularity = nx_comm.modularity(self.current_network, communities)
            
            self.community_detection = {
                'communities': communities,
                'algorithm': algorithm,
                'modularity': modularity
            }
            
            self._update_community_display()
            ui.notify(f'Found {len(communities)} communities using {algorithm} (modularity: {modularity:.3f})', type='positive')
            
        except Exception as e:
            logger.error(f"Error detecting communities: {e}")
            ui.notify(f'Community detection failed: {str(e)}', type='negative')

    def _update_community_display(self):
        """Update community detection display with real results"""
        self.community_container.clear()
        
        if not self.community_detection:
            with self.community_container:
                ui.label('No communities detected').classes('text-gray-500 italic')
            return
        
        communities = self.community_detection['communities']
        modularity = self.community_detection['modularity']
        algorithm = self.community_detection['algorithm']
        
        with self.community_container:
            with ui.row().classes('items-center gap-4 mb-4'):
                ui.label(f'Algorithm: {algorithm.title()}').classes('font-medium')
                ui.badge(f'Modularity: {modularity:.3f}', color='green').classes('px-2 py-1')
                ui.badge(f'{len(communities)} Communities', color='blue').classes('px-2 py-1')
            
            for i, community in enumerate(communities):
                with ui.expansion(f'Community {i+1} ({len(community)} entities)', icon='group').classes('w-full mb-2'):
                    with ui.column().classes('p-4'):
                        # Show community members with real entity names
                        community_entities = []
                        for node in list(community)[:15]:  # Show first 15 entities
                            node_data = self.current_network.nodes.get(node, {})
                            entity_name = node_data.get('name', node)
                            risk_score = node_data.get('risk_score', 0)
                            is_pep = node_data.get('is_pep', False)
                            
                            status_indicator = "🔴" if is_pep else ("🟡" if risk_score >= 50 else "🟢")
                            community_entities.append(f"{status_indicator} {entity_name}")
                        
                        if len(community) > 15:
                            community_entities.append(f"... and {len(community) - 15} more entities")
                        
                        ui.label("Community Members:").classes('font-medium mb-2')
                        for entity in community_entities:
                            ui.label(entity).classes('text-sm ml-4')

    def _calculate_centralities(self):
        """Calculate centrality measures using real algorithms"""
        if not self.current_network:
            ui.notify('Generate a network first', type='warning')
            return
        
        try:
            centrality_measure = self.centrality_select.value
            
            if centrality_measure == 'degree':
                centralities = nx.degree_centrality(self.current_network)
            elif centrality_measure == 'betweenness':
                centralities = nx.betweenness_centrality(self.current_network)
            elif centrality_measure == 'closeness':
                centralities = nx.closeness_centrality(self.current_network)
            elif centrality_measure == 'eigenvector':
                try:
                    centralities = nx.eigenvector_centrality(self.current_network, max_iter=1000)
                except:
                    centralities = nx.degree_centrality(self.current_network)
                    ui.notify('Eigenvector centrality failed, showing degree centrality instead', type='warning')
            else:  # pagerank
                centralities = nx.pagerank(self.current_network)
            
            self.centrality_measures = {
                'measure': centrality_measure,
                'values': centralities
            }
            
            self._update_centrality_display()
            ui.notify(f'Calculated {centrality_measure} centrality for {len(centralities)} entities', type='positive')
            
        except Exception as e:
            logger.error(f"Error calculating centralities: {e}")
            ui.notify(f'Centrality calculation failed: {str(e)}', type='negative')

    def _update_centrality_display(self):
        """Update centrality analysis display with real results"""
        self.centrality_container.clear()
        
        if not self.centrality_measures:
            with self.centrality_container:
                ui.label('No centrality measures calculated').classes('text-gray-500 italic')
            return
        
        measure = self.centrality_measures['measure']
        values = self.centrality_measures['values']
        
        # Sort by centrality value
        sorted_nodes = sorted(values.items(), key=lambda x: x[1], reverse=True)
        
        with self.centrality_container:
            ui.label(f'Top 15 entities by {measure} centrality:').classes('font-medium mb-3')
            
            # Create table data with real entity information
            columns = [
                {'name': 'rank', 'label': 'Rank', 'field': 'rank'},
                {'name': 'entity', 'label': 'Entity Name', 'field': 'entity'},
                {'name': 'centrality', 'label': f'{measure.title()} Score', 'field': 'centrality'},
                {'name': 'risk', 'label': 'Risk Score', 'field': 'risk'},
                {'name': 'type', 'label': 'Type', 'field': 'type'}
            ]
            
            rows = []
            for i, (node, centrality) in enumerate(sorted_nodes[:15]):
                node_data = self.current_network.nodes.get(node, {})
                entity_name = node_data.get('name', node)
                risk_score = node_data.get('risk_score', 0)
                entity_type = node_data.get('entity_type', 'Unknown')
                is_pep = node_data.get('is_pep', False)
                
                # Truncate long names
                if len(entity_name) > 30:
                    entity_name = entity_name[:27] + "..."
                
                # Add PEP indicator
                if is_pep:
                    entity_name = "🔴 " + entity_name
                
                rows.append({
                    'rank': i + 1,
                    'entity': entity_name,
                    'centrality': f'{centrality:.4f}',
                    'risk': risk_score,
                    'type': entity_type
                })
            
            ui.table(columns=columns, rows=rows, row_key='rank').classes('w-full')

# Global instance
advanced_network_analysis = AdvancedNetworkAnalysis()