#!/usr/bin/env ruby
require 'soda'

raise 'SocrataAppTokenEnvironmentVariableUnset' if ENV['SOCRATA_APP_TOKEN'].nil?

# Configure the dataset ID and initialize SODA client
dataset_resource_id = "c9c8-p6iz" # URL: https://ben.demo.socrata.com/d/c9c8-p6iz - private dataset, requires authentication
soda_client = SODA::Client.new({
  domain: "ben.demo.socrata.com",
  username: ENV['SOCRATA_USERNAME'],
  password: ENV['SOCRATA_PASSWORD'],
  app_token: ENV['SOCRATA_APP_TOKEN']
})


SCHEDULER.every '1440m', first_in: 0 do |job|

  # Count total number of customers - SODA query #

  total_customers_response = soda_client.get(dataset_resource_id, {
    "$select" => "count(*)"
    })
    total_customers = total_customers_response.first["count"].to_i
    send_event('total_customers', { current:  total_customers})

  # Total CMRR #

  cmrr_response = soda_client.get(dataset_resource_id, {
    "$select" => "SUM(cmrr)"
    })
    cmrr = cmrr_response.first["sum_cmrr"].to_i
    send_event('cmrr', { current: cmrr })

  # Percentage of CMRR target #

  percent_cmrr = ((cmrr.to_f/108000)*100).to_i
  send_event('percent_cmrr', { value:  percent_cmrr})

  # Build SODA query - group customers by state #

  state_response = soda_client.get(dataset_resource_id, {
    "$group" => "state",
    "$select" => "state, COUNT(state) AS n"
    })

  # Make a list #
  state = {}
  state_response.each do |item|
    state[item.state] = {:label => item.state, :value => item.n}
  end
  # Send event to the dashboard, sorting the list by value #
  send_event('state', { items: state.values.sort_by{|x| x[:value].to_i}.reverse })

  end