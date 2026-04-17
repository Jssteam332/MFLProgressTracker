const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MFL_BASE = 'https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players';
const LIMIT = 1500;
const BATCH_SIZE = 500;

async function fetchAllPlayers() {
  let allPlayers = [];
  let cursor = null;
  let page = 0;

  console.log('Starting MFL player fetch...');

  while (true) {
    let url = `${MFL_BASE}?limit=${LIMIT}&sorts=metadata.overall&sortsOrders=DESC`;
    if (cursor) url += `&beforePlayerId=${cursor}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error(`MFL API error: ${res.status}`);
    const data = await res.json();

    if (data.key && data.message) throw new Error(`MFL API: ${data.message}`);

    allPlayers.push(...data);
    page++;

    const pct = ((allPlayers.length / 200000) * 100).toFixed(1);
    console.log(`Page ${page} | Players: ${allPlayers.length.toLocaleString()} | ~${pct}%`);

    if (data.length < LIMIT) {
      console.log(`Fetch complete. ${allPlayers.length.toLocaleString()} total players.`);
      break;
    }

    cursor = data[data.length - 1].id;
    await new Promise(r => setTimeout(r, 2000));
  }

  return allPlayers;
}

function transformPlayer(p) {
  const m = p.metadata || {};
  const club = p.activeContract?.club || {};
  const owner = p.ownedBy || {};
  const offerLabels = { 0: 'Not available', 1: 'Unspecified', 2: 'Open' };

  return {
    ID: p.id,
    first_name: m.firstName || '',
    last_name: m.lastName || '',
    overall: m.overall || 0,
    position: (m.positions || [])[0] || '',
    age: m.age || 0,
    nationality: (m.nationalities || [])[0] || '',
    foot: m.preferredFoot || '',
    pace: m.pace || 0,
    shooting: m.shooting || 0,
    passing: m.passing || 0,
    dribbling: m.dribbling || 0,
    defense: m.defense || 0,
    physical: m.physical || 0,
    club: club.name || '',
    club_division: club.division || null,
    owner: owner.name || '',
    wallet: owner.walletAddress || '',
    offer_status: offerLabels[p.offerStatus] ?? String(p.offerStatus ?? ''),
  };
}

async function upsertBatch(players) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/mfl_players_current`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Prefer': 'resolution=merge-duplicates',
    },
    body: JSON.stringify(players),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase upsert error: ${err}`);
  }
}

async function insertNewToSos(players) {
  console.log('Checking for new players to add to mfl_players_sos...');
  let existingIds = new Set();
  let sosOffset = 0;
  const SOS_PAGE_SIZE = 1000;

  while (true) {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/mfl_players_sos?select=ID&limit=${SOS_PAGE_SIZE}&offset=${sosOffset}`,
      { headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` } }
    );
    const data = await res.json();
    data.forEach(p => existingIds.add(p.ID));
    if (data.length < SOS_PAGE_SIZE) break;
    sosOffset += SOS_PAGE_SIZE;
    await new Promise(r => setTimeout(r, 750));
  }

  console.log(`mfl_players_sos has ${existingIds.size.toLocaleString()} existing players.`);

  const newPlayers = players.filter(p => !existingIds.has(p.ID));
  console.log(`Found ${newPlayers.length.toLocaleString()} new players to add to mfl_players_sos.`);

  if (newPlayers.length === 0) {
    console.log('No new players to add to mfl_players_sos.');
    return;
  }

  for (let i = 0; i < newPlayers.length; i += BATCH_SIZE) {
    const batch = newPlayers.slice(i, i + BATCH_SIZE);
    const res = await fetch(`${SUPABASE_URL}/rest/v1/mfl_players_sos`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Prefer': 'resolution=ignore-duplicates',
      },
      body: JSON.stringify(batch),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Supabase SOS insert error: ${err}`);
    }

    console.log(`SOS: inserted ${Math.min(i + BATCH_SIZE, newPlayers.length).toLocaleString()} / ${newPlayers.length.toLocaleString()} new players.`);
    await new Promise(r => setTimeout(r, 300));
  }

  console.log('Done adding new players to mfl_players_sos.');
}

async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_KEY environment variables');
  }

  const players = await fetchAllPlayers();
  const transformed = players.map(transformPlayer);

  // Step 1 — upsert all players to mfl_players_current
  console.log(`Upserting ${transformed.length.toLocaleString()} players to mfl_players_current...`);
  for (let i = 0; i < transformed.length; i += BATCH_SIZE) {
    const batch = transformed.slice(i, i + BATCH_SIZE);
    await upsertBatch(batch);
    console.log(`Upserted ${Math.min(i + BATCH_SIZE, transformed.length).toLocaleString()} / ${transformed.length.toLocaleString()}`);
    await new Promise(r => setTimeout(r, 2000));
  }
  console.log('Done upserting to mfl_players_current.');

  // Step 2 — add any brand new players to mfl_players_sos as their baseline
  await insertNewToSos(transformed);

  console.log('All done!');
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
