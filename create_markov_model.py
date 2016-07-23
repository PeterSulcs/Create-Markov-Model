import pandas as pd
import numpy as np

data = pd.read_csv('ClaimsSample.csv')

# Get list of unique states (must be exhaustive!):
states = data.CLCL_STATUS.unique().tolist()
states.sort()
states = [str(state) for state in states]
states.append('End')
the_states = ['Start']
the_states.extend(states)
print('The states: {}'.format(the_states))

# Make sure the date column is in a sortable format:
data = data.assign(DATE=pd.to_datetime(data.DATE))
# Sort so that data will be sorted by DATE ascending when retrieving from groupby operation
data = data.sort_values(by='DATE')

# Establish a matrix for state transitions:
transitions = np.zeros(shape=(len(the_states), len(the_states)), dtype=float)
days_for_transition = np.zeros(shape=(len(the_states), len(the_states)), dtype=float)
print(transitions.shape)

# Iterate through each CLCL_ID:
for the_id, d in data.groupby('CLCL_ID'):
    print("Processing id = {}".format(the_id))
    state_start = "Start"
    # Iterate through each record in chronological order
    for i, row in d.iterrows():
        
        state_end = str(row['CLCL_STATUS'])
        start_ind = the_states.index(state_start)
        date_end = row['DATE']
        end_ind = the_states.index(state_end)

        if state_start == "Start":
            days = 0.0
        else:
            days = (date_end - date_start).days  # give days as a float

        transitions[start_ind, end_ind] += 1.0
        days_for_transition[start_ind, end_ind] += days
        print(" --- id {} transitioned from {} to {} in {}".format(the_id, state_start, state_end, days))
        # transition recorded, so set ending state back to the starting state to prepare for next transition
        date_start = date_end
        state_start = state_end
    
    # record transition to end state:
    print(" --- id {} transitioned from {} to {}".format(the_id, state_start, 'End'))
    final_ind = the_states.index('End')
    transitions[end_ind, final_ind] += 1
    days_for_transition[end_ind, final_ind] += 0  # assume this happens immediately

# normalize the transition matrix
trans_df = pd.DataFrame(data=transitions, columns=the_states, index=the_states)
trans_df.to_csv('transitions.csv')
print(trans_df)
days_df = pd.DataFrame(data=days_for_transition, columns=the_states, index=the_states)
days_df.to_csv('days.csv')
print(days_df)

transitions_norm = transitions * 1/transitions.sum(axis=1, keepdims=True)
normalized_days = days_for_transition/transitions

pd.DataFrame(data=transitions_norm, columns=the_states, index=the_states).to_csv('normalized_transitions.csv')
pd.DataFrame(data=normalized_days, columns=the_states, index=the_states).to_csv('normalized_days.csv')
