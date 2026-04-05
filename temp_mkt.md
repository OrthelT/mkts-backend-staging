# Temporary Market App

My Eve Online coalition (Winter coalition) has a major battle coming up on Monday. I am temporarily establishing a market hub to support this fight. I am staging the WC Seige Doctrine on the market in the VSJ Keepstar (VSJ-PP - SneakStar), structure-id: 1053176537595 

I'm not quite sure of the best way to do this. I created this worktree to avoid causing any mayham in our production workflow. This repo is the backend for our production web app, which I also have set up in a worktree of its own at ~/workspace/github/wcmkts_vsj/

What I thought we might do is to use the remote staging repos I have for the backend and the production app. Then, we switch out the secondary market to VSJ-PP, but leave our production app running as normal. I can then point Streamlit at the staging repo and have a discreet second app that displays our primary market and the staging market. This will be for only a few people to use. The general public will still be using the main app as normal. 

- We will only be staging one doctrine, doctrine_id 991, WC Siege, which I have configured in wcmktprod.db.
- We should probably create a new db for this. maybe we could just copy wcmktprod.db and make it wcmktvsj.db.
- Then we would use it as a drop-in replacement for wcmktnorth2.db and wire our secondary market up to VSJ-PP rather than B-9C24.
- I have created the db and updated .env and settings.toml with the info for the db. But, you will need to update the rest of the code to use database alias wcmktvsj instead of wcmktnorth2. 
- We will need to do the same in the frontend .Streamlit/secrets.toml file. 

## Elements:
### Backend
- VSJ market backend (this repo) ~/workspace/github/mkts_vsj/
- Tracks remote: https://github.com/OrthelT/mkts-backend-staging

### Frontend
- VSJ market frontend: ~/workspace/github/wcmkts_vsj/
- Tracks remote: git@github.com:OrthelT/wcmkts-staging.git

Make a plan to implement this. 

