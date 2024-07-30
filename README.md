<template>
  <v-dialog v-model="internaldialogVisible" max-width="1200px">
    <v-card>
      <v-card-title class="headline bg-primary white--text d-flex justify-space-between">
        App Store
        <v-btn icon @click="closeDialog" color="white">
          <v-icon>mdi-close</v-icon>
        </v-btn>
      </v-card-title>
      <v-card-text class="pa-4">
        <v-text-field
          v-model="search"
          append-icon="mdi-magnify"
          label="Search Workflow"
          single-line
          hide-details
          class="mb-4 custom-font"
        ></v-text-field>
        <v-data-table
          :headers="headers"
          :items="filteredItems"
          :search="search"
          class="elevation-1 custom-table"
          :items-per-page="10"
          :footer-props="{
            'items-per-page-options': [10, 20, 50, 100, -1],
            'items-per-page-text': 'Rows per page:',
          }"
          :sort-by.sync="sortBy"
          :sort-desc.sync="sortDesc"
        >
          <template v-slot:header="{ props: { headers } }">
            <tr>
              <th
                v-for="header in headers"
                :key="header.value"
                class="text-left custom-header"
              >
                {{ header.text }}
              </th>
            </tr>
          </template>
          <template v-slot:item="{ item }">
            <tr>
              <td v-for="header in headers" :key="header.value" class="custom-font">
                <template v-if="header.value === 'actions'">
                  <v-icon small class="mr-2" @click="editItem(item)">mdi-pencil</v-icon>
                  <v-icon small @click="deleteItem(item)">mdi-delete</v-icon>
                </template>
                <template v-else-if="header.value === 'isActive'">
                  <v-chip :color="item.isActive ? 'green' : 'red'" small>
                    {{ item.isActive ? 'Yes' : 'No' }}
                  </v-chip>
                </template>
                <template v-else>
                  {{ item[header.value] }}
                </template>
              </td>
            </tr>
          </template>
        </v-data-table>
      </v-card-text>
    </v-card>

    <v-dialog v-model="editDialog" max-width="600px">
      <v-card>
        <v-card-title class="headline bg-primary white--text d-flex justify-space-between">
          {{ editedIndex === -1 ? 'New Item' : 'Edit Item' }}
          <v-btn icon @click="closeEditDialog" color="white">
            <v-icon>mdi-close</v-icon>
          </v-btn>
        </v-card-title>
        <v-card-text class="pt-4">
          <v-form ref="form" v-model="valid">
            <v-text-field v-model="form.workflow_name" label="Workflow Name" disabled class="custom-font"></v-text-field>
            <v-text-field v-model="form.system" label="System" disabled class="custom-font"></v-text-field>
            <v-text-field v-model="form.url" label="URL" :rules="[v => !!v || 'URL is required']" required class="custom-font"></v-text-field>
            <v-text-field v-model="form.title" label="Title" :rules="[v => !!v || 'Title is required']" required class="custom-font"></v-text-field>
            <v-select v-model="form.environment" :items="['UAT', 'Testing', 'Production']" label="Environment" :rules="[v => !!v || 'Environment is required']" required class="custom-font"></v-select>
            <v-switch v-model="form.isActive" label="Is Active" class="custom-font"></v-switch>
          </v-form>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn color="red darken-1" text @click="closeEditDialog">Cancel</v-btn>
          <v-btn @click="submitForm" color="blue darken-1" :disabled="!valid">Update</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-dialog>
</template>

<script>
// ... existing script ...
</script>

<style scoped>
.custom-table {
  border: 1px solid #e0e0e0;
  border-radius: 4px;
}

.custom-table .v-data-table__wrapper {
  max-height: 400px;
  overflow-y: auto;
}

.custom-header {
  font-family: 'Gill Sans', sans-serif !important;
  font-weight: bold !important;
  font-size: 14px !important;
  text-transform: uppercase;
  white-space: nowrap;
  padding: 0 16px;
}

.custom-font {
  font-family: 'Gill Sans', sans-serif !important;
  font-size: 14px !important;
}

.v-data-table > .v-data-table__wrapper > table > tbody > tr > td {
  border-bottom: 1px solid #e0e0e0;
  padding: 0 16px;
}

.v-data-table > .v-data-table__wrapper > table > tbody > tr:hover {
  background-color: #f5f5f5 !important;
}

.headline {
  font-size: 1.5rem;
  font-weight: 500;
}
</style>